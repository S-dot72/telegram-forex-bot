"""
Production bot qui charge best_params.json si présent pour appliquer les paramètres optimisés par pair.
Programme SIGNALS_PER_DAY et envoie chaque pré-signal GAP_MIN_BEFORE_ENTRY minutes avant l'entrée.
Support multi-utilisateurs via table subscribers.
"""

import os, json, asyncio
from datetime import datetime, timedelta, timezone, time as dtime
import requests
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import create_engine, text
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from config import *
from utils import compute_indicators, rule_signal

# --- Database ---
engine = create_engine(DB_URL, connect_args={'check_same_thread': False})

# --- Charger les meilleurs paramètres si présents ---
BEST_PARAMS = {}
if os.path.exists(BEST_PARAMS_FILE):
    try:
        with open(BEST_PARAMS_FILE, 'r') as f:
            BEST_PARAMS = json.load(f)
    except Exception:
        BEST_PARAMS = {}

TWELVE_TS_URL = 'https://api.twelvedata.com/time_series'

# --- Fonctions utilitaires ---

def fetch_ohlc_td(pair, interval, outputsize=300):
    params = {'symbol': pair, 'interval': interval, 'outputsize': outputsize,
              'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TWELVE_TS_URL, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if 'values' not in j:
        raise RuntimeError(f"TwelveData error: {j}")
    df = pd.DataFrame(j['values'])[::-1].reset_index(drop=True)
    
    # Convertir seulement les colonnes disponibles
    required_cols = ['open', 'high', 'low', 'close']
    for col in required_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)
    
    # Volume est optionnel pour le forex
    if 'volume' in df.columns:
        df['volume'] = df['volume'].astype(float)
    
    df.index = pd.to_datetime(df['datetime'])
    return df

def persist_signal(payload):
    q = text("INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json) "
             "VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload)")
    with engine.begin() as conn:
        conn.execute(q, payload)

def generate_daily_schedule_for_today():
    today = datetime.utcnow().date()
    start_dt = datetime.combine(today, dtime(START_HOUR_UTC, 0, 0), tzinfo=timezone.utc)
    end_dt = datetime.combine(today, dtime(END_HOUR_UTC, 0, 0), tzinfo=timezone.utc)
    total_minutes = int((end_dt - start_dt).total_seconds()//60)
    interval = max(1, total_minutes // SIGNALS_PER_DAY)
    schedule = []
    for i in range(SIGNALS_PER_DAY):
        t = start_dt + timedelta(minutes=i*interval)
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

# --- Commandes Telegram ---

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    print(f"📥 /start reçu de user_id={user_id} username={username}")
    try:
        with engine.begin() as conn:
            # Vérifier si déjà abonné
            existing = conn.execute(
                text("SELECT user_id FROM subscribers WHERE user_id = :uid"),
                {"uid": user_id}
            ).fetchone()
            
            if existing:
                await update.message.reply_text("✅ Vous êtes déjà abonné aux signaux !")
                print(f"ℹ️  User {user_id} déjà abonné")
            else:
                conn.execute(
                    text("INSERT INTO subscribers (user_id, username) VALUES (:uid, :uname)"),
                    {"uid": user_id, "uname": username}
                )
                await update.message.reply_text(
                    "✅ Bienvenue ! Vous êtes maintenant abonné aux signaux de trading.\n\n"
                    "📊 Vous recevrez automatiquement les signaux pendant les heures de trading.\n\n"
                    "Commandes disponibles:\n"
                    "/stats - Voir les statistiques\n"
                    "/result <timestamp> <WIN|LOSE> - Enregistrer un résultat"
                )
                print(f"✅ User {user_id} ajouté aux abonnés")
    except Exception as e:
        print(f"❌ Erreur dans cmd_start: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(f"❌ Erreur: {e}")

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
        await update.message.reply_text('✅ Résultat mis à jour')
    except Exception as e:
        await update.message.reply_text('❌ Erreur: '+str(e))

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with engine.connect() as conn:
        total = conn.execute(text('SELECT COUNT(*) FROM signals')).scalar()
        wins = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result='WIN'")).scalar()
        subs = conn.execute(text('SELECT COUNT(*) FROM subscribers')).scalar()
    winrate = (wins/total*100) if total > 0 else 0
    await update.message.reply_text(
        f"📊 **Statistiques**\n\n"
        f"Total signaux: {total}\n"
        f"Victoires: {wins}\n"
        f"Taux de réussite: {winrate:.1f}%\n"
        f"Abonnés: {subs}"
    )

# --- Envoi de signaux à tous les abonnés ---

async def send_pre_signal(pair, entry_time, app):
    print(f"🔄 Génération du signal pour {pair} à {datetime.utcnow()}")
    try:
        # Récupération des paramètres optimisés
        params = BEST_PARAMS.get(pair, {})
        ema_f = params.get('ema_fast', 8)
        ema_s = params.get('ema_slow', 21)
        rsi_l = params.get('rsi', 14)
        bb_l = params.get('bb', 20)

        # Calcul du signal
        print(f"📊 Récupération des données pour {pair}...")
        df = fetch_ohlc_td(pair, TIMEFRAME_M1, outputsize=400)
        print(f"✅ {len(df)} bougies récupérées")
        
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

        print(f"📍 Direction: {direction}, Confiance: {confidence}")

        # Persister dans la DB
        ts_send = datetime.utcnow().replace(tzinfo=timezone.utc)
        payload = {
            'pair': pair,
            'direction': direction,
            'reason': reason,
            'ts_enter': entry_time.isoformat(),
            'ts_send': ts_send.isoformat(),
            'confidence': confidence,
            'payload': json.dumps({'pair': pair,'reason': reason})
        }
        persist_signal(payload)
        print(f"💾 Signal sauvegardé dans la DB")

        # Récupérer tous les abonnés
        with engine.connect() as conn:
            user_ids = [row[0] for row in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]

        print(f"👥 {len(user_ids)} abonné(s) trouvé(s)")

        if not user_ids:
            print("⚠️  Aucun abonné, signal non envoyé")
            return

        msg = format_signal_message(pair, direction, entry_time, confidence, reason)

        # Envoyer le message à tous les abonnés
        sent_count = 0
        for uid in user_ids:
            try:
                await app.bot.send_message(chat_id=uid, text=msg)
                sent_count += 1
                print(f"✅ Signal envoyé à user {uid}")
            except Exception as e:
                print(f"❌ Erreur envoi à user {uid}: {e}")

        print(f"✅ Signal envoyé à {sent_count}/{len(user_ids)} utilisateurs pour {pair}")
    except Exception as e:
        print(f'❌ Erreur en envoyant le signal: {e}')
        import traceback
        traceback.print_exc()

# --- Scheduler ---

async def schedule_today_signals(app, sched):
    if datetime.utcnow().weekday() > 4:
        print('🏖️  Weekend, aucun signal')
        return

    sched.remove_all_jobs()
    daily = generate_daily_schedule_for_today()
    for item in daily:
        entry = item['entry_time']
        send_time = entry - timedelta(minutes=GAP_MIN_BEFORE_ENTRY)
        if send_time > datetime.utcnow().replace(tzinfo=timezone.utc):
            sched.add_job(send_pre_signal, 'date', run_date=send_time, args=[item['pair'], entry, app])
    print(f"📅 {len(daily)} signaux planifiés pour aujourd'hui")

# --- Création DB si nécessaire ---

def ensure_db():
    sql = open('db_schema.sql').read()
    with engine.begin() as conn:
        for stmt in sql.split(';'):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

# --- Main ---

async def send_all_signals_now(app):
    """Envoie tous les signaux immédiatement pour test, avec délai pour respecter les limites API"""
    print("🚀 Envoi immédiat de tous les signaux pour test...")
    daily = generate_daily_schedule_for_today()
    
    for i, item in enumerate(daily, 1):
        print(f"📤 Envoi signal {i}/{len(daily)} pour {item['pair']}...")
        await send_pre_signal(item['pair'], item['entry_time'], app)
        
        # Attendre 8 secondes entre chaque signal pour respecter la limite API (8 req/min)
        if i < len(daily):
            print(f"⏳ Attente de 10 secondes pour respecter la limite API...")
            await asyncio.sleep(10)
    
    print("✅ Tous les signaux ont été envoyés.")

async def main():
    print("🚀 Démarrage du bot...")
    ensure_db()

    # Créer l'application
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Ajouter les handlers
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('result', cmd_result))
    app.add_handler(CommandHandler('stats', cmd_stats))

    # Créer le scheduler APRÈS avoir démarré l'event loop
    sched = AsyncIOScheduler(timezone='UTC')
    sched.start()
    print("⏰ Scheduler démarré")
    
    # Planifier les signaux d'aujourd'hui
    await schedule_today_signals(app, sched)
    
    # Ajouter le job quotidien
    sched.add_job(schedule_today_signals, 'cron', hour=8, minute=55, args=[app, sched])
    print("📆 Job quotidien configuré")

    # Démarrer le bot
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)  # drop_pending_updates=True évite les conflits
    
    print("✅ Bot démarré avec succès!")
    print(f"🤖 Bot: @{(await app.bot.get_me()).username}")
    
    # 🔥 ENVOYER TOUS LES SIGNAUX IMMÉDIATEMENT POUR TEST 🔥
    print("\n⚡ MODE TEST : Envoi immédiat de tous les signaux...")
    await send_all_signals_now(app)
    
    # Garder le bot en vie
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 Arrêt du bot...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        sched.shutdown()
        print("👋 Bot arrêté")

if __name__ == '__main__':
    asyncio.run(main())
