"""
Production bot avec ML et vérification automatique
- Signaux à 9h UTC (corrigé pour Railway UTC-5)
- Vérification APRÈS chaque signal avec rapport individuel
- 1 signal → vérification → rapport → signal suivant
"""

import os, json, asyncio
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import create_engine, text
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from config import *
from utils import compute_indicators, rule_signal
from ml_predictor import MLSignalPredictor
from auto_verifier import AutoResultVerifier

# --- Configuration ---
START_HOUR_UTC = 9
SIGNAL_INTERVAL_MIN = 5
DELAY_BEFORE_ENTRY_MIN = 3
NUM_SIGNALS_PER_DAY = 20

# --- Database et scheduler (HEURE LOCALE DU SYSTÈME) ---
engine = create_engine(DB_URL, connect_args={'check_same_thread': False})
sched = AsyncIOScheduler()  # PAS de timezone, utilise l'heure locale

# --- ML et Verifier ---
ml_predictor = MLSignalPredictor()
auto_verifier = None
current_signal_index = 0  # Pour suivre quel signal on envoie

BEST_PARAMS = {}
if os.path.exists(BEST_PARAMS_FILE):
    try:
        with open(BEST_PARAMS_FILE, 'r') as f:
            BEST_PARAMS = json.load(f)
    except:
        pass

TWELVE_TS_URL = 'https://api.twelvedata.com/time_series'
ohlc_cache = {}
CACHE_DURATION_SECONDS = 60

# --- Fonctions utilitaires ---

def get_utc_now():
    """Retourne l'heure UTC réelle"""
    return datetime.now(timezone.utc)

def utc_to_local(utc_dt):
    """Convertit UTC vers heure locale système"""
    return utc_dt.replace(tzinfo=timezone.utc).astimezone()

def local_to_utc(local_dt):
    """Convertit heure locale vers UTC"""
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=None)
        return datetime.fromtimestamp(local_dt.timestamp(), tz=timezone.utc)
    return local_dt.astimezone(timezone.utc)

def fetch_ohlc_td(pair, interval, outputsize=300):
    params = {'symbol': pair, 'interval': interval, 'outputsize': outputsize,
              'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TWELVE_TS_URL, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if 'values' not in j:
        raise RuntimeError(f"TwelveData error: {j}")
    df = pd.DataFrame(j['values'])[::-1].reset_index(drop=True)
    for col in ['open','high','low','close']:
        if col in df.columns:
            df[col] = df[col].astype(float)
    if 'volume' in df.columns:
        df['volume'] = df['volume'].astype(float)
    df.index = pd.to_datetime(df['datetime'])
    return df

def get_cached_ohlc(pair, interval, outputsize=300):
    cache_key = f"{pair}_{interval}"
    current_time = get_utc_now()
    if cache_key in ohlc_cache:
        cached_data, cached_time = ohlc_cache[cache_key]
        if (current_time - cached_time).total_seconds() < CACHE_DURATION_SECONDS:
            return cached_data
    df = fetch_ohlc_td(pair, interval, outputsize)
    ohlc_cache[cache_key] = (df, current_time)
    return df

def persist_signal(payload):
    q = text("""INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json)
                VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload)""")
    with engine.begin() as conn:
        conn.execute(q, payload)

def format_signal_message(pair, direction, entry_time_utc, confidence):
    direction_text = "BUY" if direction == "CALL" else "SELL"
    gale1 = entry_time_utc + timedelta(minutes=5)
    gale2 = entry_time_utc + timedelta(minutes=10)
    msg = (
        f"📊 SIGNAL — {pair}\n\n"
        f"Entrée (UTC): {entry_time_utc.strftime('%H:%M')}\n\n"
        f"Direction: {direction_text}\n\n"
        f"     Gale 1: {gale1.strftime('%H:%M')}\n"
        f"     Gale 2: {gale2.strftime('%H:%M')}\n\n"
        f"Confiance: {int(confidence*100)}%"
    )
    return msg

# --- Commandes Telegram ---

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    try:
        with engine.begin() as conn:
            existing = conn.execute(text("SELECT user_id FROM subscribers WHERE user_id = :uid"),
                                    {"uid": user_id}).fetchone()
            if existing:
                await update.message.reply_text("✅ Vous êtes déjà abonné.")
            else:
                conn.execute(text("INSERT INTO subscribers (user_id, username) VALUES (:uid, :uname)"),
                             {"uid": user_id, "uname": username})
                await update.message.reply_text(
                    f"✅ Bienvenue !\n\n"
                    f"📊 {NUM_SIGNALS_PER_DAY} signaux/jour à {START_HOUR_UTC}h UTC\n"
                    f"⏱️ Signal toutes les {SIGNAL_INTERVAL_MIN}min\n"
                    f"🔍 Vérification après chaque signal"
                )
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Test de signal...")
    pair = PAIRS[0]
    entry_time_utc = get_utc_now() + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)
    await send_signal_and_verify(pair, entry_time_utc, context.application)
    await update.message.reply_text("✅ Test terminé!")

# --- Envoi et vérification ---

async def send_signal_and_verify(pair, entry_time_utc, app):
    """
    1. Envoie un signal
    2. Attend la fin du signal + gales
    3. Vérifie le résultat
    4. Envoie le rapport
    """
    now_utc = get_utc_now()
    print(f"\n{'='*60}")
    print(f"📤 ENVOI SIGNAL - {now_utc.strftime('%H:%M:%S')} UTC")
    print(f"   Paire: {pair}")
    print(f"   Entrée: {entry_time_utc.strftime('%H:%M:%S')} UTC")
    
    try:
        # 1. Générer et envoyer le signal
        params = BEST_PARAMS.get(pair, {})
        df = get_cached_ohlc(pair, TIMEFRAME_M1, outputsize=400)
        df = compute_indicators(df, ema_fast=params.get('ema_fast',8),
                                ema_slow=params.get('ema_slow',21),
                                rsi_len=params.get('rsi',14),
                                bb_len=params.get('bb',20))
        base_signal = rule_signal(df)
        
        if not base_signal:
            print("⏭️ Pas de signal base")
            return
        
        ml_signal, ml_conf = ml_predictor.predict_signal(df, base_signal)
        if ml_signal is None or ml_conf < 0.70:
            print(f"❌ Rejeté (conf {ml_conf:.1%})")
            return
        
        # Sauvegarder en DB
        payload = {
            'pair': pair, 'direction': ml_signal, 'reason': f'ML {ml_conf:.1%}',
            'ts_enter': entry_time_utc.isoformat(), 
            'ts_send': now_utc.isoformat(),
            'confidence': ml_conf, 
            'payload': json.dumps({'pair': pair})
        }
        persist_signal(payload)
        signal_id = None
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id FROM signals WHERE ts_enter = :ts ORDER BY id DESC LIMIT 1"),
                                 {'ts': entry_time_utc.isoformat()}).fetchone()
            if result:
                signal_id = result[0]
        
        # Envoyer aux utilisateurs
        with engine.connect() as conn:
            user_ids = [r[0] for r in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]
        
        msg = format_signal_message(pair, ml_signal, entry_time_utc, ml_conf)
        for uid in user_ids:
            try:
                await app.bot.send_message(chat_id=uid, text=msg)
            except Exception as e:
                print(f"❌ Envoi à {uid}: {e}")
        
        print(f"✅ Signal envoyé: {pair} {ml_signal} ({ml_conf:.1%})")
        
        # 2. Attendre la fin du signal + gales (5min signal + 5min gale1 + 5min gale2 = 15min)
        wait_time_minutes = 5 * 3  # 5min × 3 tentatives
        wait_seconds = wait_time_minutes * 60
        end_time = entry_time_utc + timedelta(minutes=wait_time_minutes)
        
        print(f"⏳ Attente de {wait_time_minutes}min pour vérification...")
        print(f"   Fin prévue: {end_time.strftime('%H:%M:%S')} UTC")
        
        # Calculer combien de temps attendre
        time_to_wait = (end_time - get_utc_now()).total_seconds()
        if time_to_wait > 0:
            await asyncio.sleep(time_to_wait)
        
        # 3. Vérifier le résultat
        print(f"\n🔍 VÉRIFICATION du signal #{signal_id}...")
        if signal_id:
            result, details = await verify_single_signal(signal_id)
            if result:
                # 4. Envoyer le rapport individuel
                await send_individual_report(signal_id, result, details, app)
        
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()

async def verify_single_signal(signal_id):
    """Vérifie un signal spécifique et retourne le résultat"""
    try:
        query = text("""
            SELECT id, pair, direction, ts_enter, confidence
            FROM signals 
            WHERE id = :id
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {'id': signal_id}).fetchone()
        
        if not result:
            return None, None
        
        # Créer un objet signal
        class Signal:
            def __init__(self, row):
                self.id = row[0]
                self.pair = row[1]
                self.direction = row[2]
                self.ts_enter = row[3]
                self.confidence = row[4]
                self.timeframe = 5
                self.max_gales = 2
        
        signal = Signal(result)
        entry_time = datetime.fromisoformat(signal.ts_enter.replace('Z', '+00:00'))
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        
        # Tester signal initial + 2 gales
        for attempt in range(3):
            attempt_entry = entry_time + timedelta(minutes=5 * attempt)
            attempt_exit = attempt_entry + timedelta(minutes=5)
            
            entry_price = await get_price_at_time(signal.pair, attempt_entry)
            if not entry_price:
                continue
            
            await asyncio.sleep(1)
            exit_price = await get_price_at_time(signal.pair, attempt_exit)
            if not exit_price:
                continue
            
            is_winning = False
            if signal.direction == 'CALL':
                is_winning = exit_price > entry_price
            else:
                is_winning = exit_price < entry_price
            
            pips = abs(exit_price - entry_price) * 10000
            
            if is_winning:
                attempt_name = "Signal initial" if attempt == 0 else f"Gale {attempt}"
                details = {
                    'winning_attempt': attempt_name,
                    'pips': pips,
                    'entry_price': entry_price,
                    'exit_price': exit_price
                }
                
                # Sauvegarder en DB
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE signals 
                        SET result = 'WIN', 
                            ts_result = :ts_result,
                            winning_attempt = :winning_attempt
                        WHERE id = :id
                    """), {
                        'id': signal_id,
                        'ts_result': get_utc_now().isoformat(),
                        'winning_attempt': attempt_name
                    })
                
                return 'WIN', details
        
        # Toutes les tentatives ont échoué
        details = {
            'winning_attempt': None,
            'pips': pips,
            'entry_price': entry_price,
            'exit_price': exit_price
        }
        
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE signals 
                SET result = 'LOSE', 
                    ts_result = :ts_result
                WHERE id = :id
            """), {
                'id': signal_id,
                'ts_result': get_utc_now().isoformat()
            })
        
        return 'LOSE', details
        
    except Exception as e:
        print(f"❌ Erreur vérification: {e}")
        return None, None

async def get_price_at_time(pair, timestamp):
    """Récupère le prix à un moment donné"""
    try:
        end_str = (timestamp + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
        start_str = (timestamp - timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:%S')
        
        params = {
            'symbol': pair,
            'interval': '1min',
            'outputsize': 10,
            'apikey': TWELVEDATA_API_KEY,
            'format': 'JSON',
            'start_date': start_str,
            'end_date': end_str
        }
        
        response = requests.get(TWELVE_TS_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'values' in data and len(data['values']) > 0:
            closest_candle = None
            min_diff = float('inf')
            
            for candle in data['values']:
                candle_time = datetime.strptime(candle['datetime'], '%Y-%m-%d %H:%M:%S')
                candle_time = candle_time.replace(tzinfo=timezone.utc)
                diff = abs((candle_time - timestamp).total_seconds())
                
                if diff < min_diff:
                    min_diff = diff
                    closest_candle = candle
            
            if closest_candle and min_diff < 180:
                return float(closest_candle['close'])
        
        return None
        
    except Exception as e:
        print(f"⚠️ Erreur API: {e}")
        return None

async def send_individual_report(signal_id, result, details, app):
    """Envoie un rapport pour un signal spécifique"""
    try:
        # Récupérer les infos du signal
        with engine.connect() as conn:
            signal = conn.execute(text("""
                SELECT pair, direction, confidence 
                FROM signals 
                WHERE id = :id
            """), {'id': signal_id}).fetchone()
        
        if not signal:
            return
        
        emoji = "✅" if result == 'WIN' else "❌"
        status = "RÉUSSI" if result == 'WIN' else "ÉCHOUÉ"
        
        report = f"{emoji} **RÉSULTAT DU SIGNAL**\n"
        report += "━━━━━━━━━━━━━━━━━━━━\n\n"
        report += f"Paire: {signal[0]}\n"
        report += f"Direction: {signal[1]}\n"
        report += f"Statut: **{status}**\n"
        
        if details.get('winning_attempt'):
            report += f"✅ Gagné sur: {details['winning_attempt']}\n"
        else:
            report += f"❌ Toutes les tentatives échouées\n"
        
        report += f"\n📊 Détails:\n"
        report += f"• Pips: {details['pips']:.1f}\n"
        report += f"• Confiance: {signal[2]:.0%}\n"
        report += "\n━━━━━━━━━━━━━━━━━━━━"
        
        # Envoyer à tous les abonnés
        with engine.connect() as conn:
            user_ids = [r[0] for r in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]
        
        for uid in user_ids:
            try:
                await app.bot.send_message(chat_id=uid, text=report)
            except Exception as e:
                print(f"❌ Envoi rapport à {uid}: {e}")
        
        print(f"✅ Rapport envoyé: {result}")
        
    except Exception as e:
        print(f"❌ Erreur rapport: {e}")

# --- Scheduler ---

async def send_daily_signals_sequence(app):
    """Envoie les signaux un par un avec vérification entre chaque"""
    global current_signal_index
    
    now_utc = get_utc_now()
    
    # Calculer quand 9h UTC en heure locale
    today_local = datetime.now()
    target_utc = datetime.combine(now_utc.date(), datetime.min.time()).replace(hour=START_HOUR_UTC, tzinfo=timezone.utc)
    target_local = utc_to_local(target_utc)
    
    print(f"\n📅 PLANNING QUOTIDIEN")
    print(f"   UTC: {target_utc.strftime('%Y-%m-%d %H:%M')}")
    print(f"   Local: {target_local.strftime('%Y-%m-%d %H:%M')}")
    
    active_pairs = PAIRS[:2]
    
    for i in range(NUM_SIGNALS_PER_DAY):
        # Calculer l'heure d'envoi en UTC
        send_time_utc = target_utc + timedelta(minutes=i * SIGNAL_INTERVAL_MIN)
        entry_time_utc = send_time_utc + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)
        pair = active_pairs[i % len(active_pairs)]
        
        # Convertir en heure locale pour le scheduler
        send_time_local = utc_to_local(send_time_utc)
        
        # Planifier
        sched.add_job(
            send_signal_and_verify,
            'date',
            run_date=send_time_local,
            args=[pair, entry_time_utc, app],
            id=f"signal_{i}"
        )
    
    print(f"✅ {NUM_SIGNALS_PER_DAY} signaux planifiés\n")

# --- DB ---

def ensure_db():
    sql = open('db_schema.sql').read()
    with engine.begin() as conn:
        for stmt in sql.split(';'):
            if stmt.strip():
                conn.execute(text(stmt.strip()))

# --- Main ---

async def main():
    global auto_verifier
    
    now_local = datetime.now()
    now_utc = get_utc_now()
    offset = (now_local - now_utc.replace(tzinfo=None)).total_seconds() / 3600
    
    print("\n" + "="*60)
    print("🤖 DÉMARRAGE DU BOT")
    print("="*60)
    print(f"🕐 Heure locale: {now_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌍 Heure UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 Offset: UTC{offset:+.0f}")
    print(f"⏰ Premier signal: {START_HOUR_UTC}h00 UTC")
    print("="*60 + "\n")
    
    ensure_db()
    auto_verifier = AutoResultVerifier(engine, TWELVEDATA_API_KEY)
    
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('test', cmd_test))

    sched.start()
    
    # Planifier les signaux du jour
    await send_daily_signals_sequence(app)
    
    # Job quotidien à 8h55 UTC (converti en heure locale)
    target_utc = datetime.now(timezone.utc).replace(hour=8, minute=55, second=0, microsecond=0)
    target_local = utc_to_local(target_utc)
    
    sched.add_job(
        send_daily_signals_sequence,
        'cron',
        hour=target_local.hour,
        minute=target_local.minute,
        args=[app],
        id='daily_schedule'
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    
    print("✅ BOT OPÉRATIONNEL\n")
    
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())
