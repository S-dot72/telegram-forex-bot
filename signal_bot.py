"""Production bot that loads best_params.json if present to apply optimized params per pair.
Schedules SIGNALS_PER_DAY and sends each pre-signal GAP_MIN_BEFORE_ENTRY minutes before entry.
"""
import os, time, json
from datetime import datetime, timedelta, timezone, time as dtime
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text
from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from config import *
from utils import compute_indicators, rule_signal

engine = create_engine(DB_URL, connect_args={'check_same_thread': False})
bot = Bot(token=TELEGRAM_BOT_TOKEN)
sched = BackgroundScheduler(timezone='UTC')

TWELVE_TS_URL = 'https://api.twelvedata.com/time_series'

BEST_PARAMS = {}
if os.path.exists(BEST_PARAMS_FILE):
    try:
        with open(BEST_PARAMS_FILE,'r') as f:
            BEST_PARAMS = json.load(f)
    except Exception:
        BEST_PARAMS = {}


def fetch_ohlc_td(pair, interval, outputsize=300):
    symbol = pair.replace('/','')
    params = {'symbol': symbol, 'interval': interval, 'outputsize': outputsize, 'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TWELVE_TS_URL, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if 'values' not in j:
        raise RuntimeError(f"TwelveData error: {j}")
    df = pd.DataFrame(j['values'])[::-1].reset_index(drop=True)
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    df.index = pd.to_datetime(df['datetime'])
    return df


def persist_signal(payload):
    q = text("INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json) VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload)")
    with engine.begin() as conn:
        conn.execute(q, payload)


def generate_daily_schedule_for_today():
    today = datetime.utcnow().date()
    start_dt = datetime.combine(today, dtime(START_HOUR_UTC,0,0), tzinfo=timezone.utc)
    end_dt = datetime.combine(today, dtime(END_HOUR_UTC,0,0), tzinfo=timezone.utc)
    total_minutes = int((end_dt - start_dt).total_seconds()//60)
    interval = max(1, total_minutes // SIGNALS_PER_DAY)
    times = []
    for i in range(SIGNALS_PER_DAY):
        t = start_dt + timedelta(minutes=i*interval)
        times.append(t)
    schedule = []
    for i, t in enumerate(times):
        pair = PAIRS[i % len(PAIRS)]
        schedule.append({'pair': pair, 'entry_time': t})
    return schedule


def format_signal_message(pair, direction, entry_time, confidence, reason):
    gale1 = entry_time + timedelta(minutes=GALE_INTERVAL_MIN)
    gale2 = entry_time + timedelta(minutes=GALE_INTERVAL_MIN*2)
    msg = (f"📊 SIGNAL — {pair}\n"
           f"⏳ Entrée (UTC): {entry_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
           f"⏰ Envoi {GAP_MIN_BEFORE_ENTRY} minutes avant l'entrée\n"
           f"➡ Direction: {direction}\n"
           f"🔎 Raison: {reason}\n"
           f"⭐ Confiance: {int(confidence*100)}%\n"
           f"💥 Gale 1: {gale1.strftime('%Y-%m-%d %H:%M:%S')}\n"
           f"💥 Gale 2: {gale2.strftime('%Y-%m-%d %H:%M:%S')}\n"
           f"⚠️ Trades du lundi au vendredi. Riskez prudemment (1% bankroll suggéré).")
    return msg

async def cmd_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 2:
            await update.message.reply_text('Usage: /result <ts_enter_iso> <WIN|LOSE>')
            return
        ts = args[0]
        res = args[1].upper()
        if res not in ('WIN','LOSE'):
            await update.message.reply_text('Result must be WIN or LOSE')
            return
        with engine.begin() as conn:
            q = text("UPDATE signals SET result=:r, ts_result=:t WHERE ts_enter=:ts")
            conn.execute(q, {'r':res, 't':datetime.utcnow().isoformat(), 'ts':ts})
        await update.message.reply_text('Updated result')
    except Exception as e:
        await update.message.reply_text('Error: '+str(e))

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with engine.connect() as conn:
        total = conn.execute(text('SELECT COUNT(*) FROM signals')).scalar()
        wins = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result='WIN'")).scalar()
    await update.message.reply_text(f'Total signals: {total} — Wins recorded: {wins}')


def send_pre_signal(pair, entry_time):
    try:
        params = BEST_PARAMS.get(pair, {})
        ema_f = params.get('ema_fast', 8)
        ema_s = params.get('ema_slow', 21)
        rsi_l = params.get('rsi', 14)
        bb_l = params.get('bb', 20)
        df = fetch_ohlc_td(pair, TIMEFRAME_M1, outputsize=400)
        df = compute_indicators(df, ema_fast=ema_f, ema_slow=ema_s, rsi_len=rsi_l, bb_len=bb_l)
        sig = rule_signal(df)
        if sig:
            direction = sig
            confidence = 0.8
            reason = f'Optimized params: EMA({ema_f},{ema_s}) RSI{rsi_l} BB{bb_l}'
        else:
            direction = 'CALL' if df['ema_fast'].iloc[-1] > df['ema_slow'].iloc[-1] else 'PUT'
            confidence = 0.35
            reason = 'fallback trend'
        ts_send = datetime.utcnow().replace(tzinfo=timezone.utc)
        payload = {
            'pair': pair,
            'direction': direction,
            'reason': reason,
            'ts_enter': entry_time.isoformat(),
            'ts_send': ts_send.isoformat(),
            'confidence': confidence,
            'payload': json.dumps({'pair':pair,'reason':reason})
        }
        persist_signal(payload)
        msg = format_signal_message(pair, direction, entry_time, confidence, reason)
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)
        print(f"Sent signal for {pair} entry {entry_time} dir {direction} conf {confidence}")
    except Exception as e:
        print('Error sending pre-signal', e)


def schedule_today_signals():
    if datetime.utcnow().weekday() > 4:
        print('Weekend, no schedule')
        return
    sched.remove_all_jobs()
    daily = generate_daily_schedule_for_today()
    for item in daily:
        entry = item['entry_time']
        send_time = entry - timedelta(minutes=GAP_MIN_BEFORE_ENTRY)
        if send_time > datetime.utcnow().replace(tzinfo=timezone.utc):
            sched.add_job(send_pre_signal, 'date', run_date=send_time, args=[item['pair'], entry])
    print('Scheduled', len(daily), 'signals for today')


def ensure_db():
    sql = open('db_schema.sql').read()
    with engine.begin() as conn:
            for stmt in sql.split(';'):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))

if __name__=='__main__':
    ensure_db()
    sched.add_job(schedule_today_signals, 'cron', hour=8, minute=55)
    schedule_today_signals()
    sched.start()
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler('result', cmd_result))
    app.add_handler(CommandHandler('stats', cmd_stats))
    import threading
    t = threading.Thread(target=app.run_polling, daemon=True)
    t.start()
    try:
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()
