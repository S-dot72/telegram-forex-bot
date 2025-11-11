"""
Production bot avec Machine Learning et vérification automatique des résultats.
- 20 signaux par jour à partir de 9h UTC
- Signal toutes les 5 minutes avec délai de 3 minutes avant entrée
- ML pour améliorer la confiance des signaux
- Vérification automatique WIN/LOSE
- Support multi-utilisateurs
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
from ml_predictor import MLSignalPredictor
from auto_verifier import AutoResultVerifier

# --- Configuration horaires ---
START_HOUR_UTC = 9  # Début à 9h UTC
SIGNAL_INTERVAL_MIN = 5  # Signal toutes les 5 minutes
DELAY_BEFORE_ENTRY_MIN = 3  # 3 minutes entre envoi et entrée
NUM_SIGNALS_PER_DAY = 20  # Nombre de signaux par jour

# --- Database et scheduler ---
engine = create_engine(DB_URL, connect_args={'check_same_thread': False})
sched = AsyncIOScheduler(timezone='UTC')

# --- ML Predictor et Auto Verifier ---
ml_predictor = MLSignalPredictor()
auto_verifier = None  # Initialisé dans main() car besoin de l'engine

# --- Charger les meilleurs paramètres si présents ---
BEST_PARAMS = {}
if os.path.exists(BEST_PARAMS_FILE):
    try:
        with open(BEST_PARAMS_FILE, 'r') as f:
            BEST_PARAMS = json.load(f)
    except Exception:
        BEST_PARAMS = {}

TWELVE_TS_URL = 'https://api.twelvedata.com/time_series'

# Cache global pour les données OHLC
ohlc_cache = {}
CACHE_DURATION_SECONDS = 60

# --- Fonctions utilitaires ---

def fetch_ohlc_td(pair, interval, outputsize=300):
    """Récupère les données OHLC depuis TwelveData API"""
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

def get_cached_ohlc(pair, interval, outputsize=300):
    """Récupère les données OHLC depuis le cache ou l'API"""
    cache_key = f"{pair}_{interval}"
    current_time = datetime.utcnow()
    
    # Vérifier si on a des données en cache valides
    if cache_key in ohlc_cache:
        cached_data, cached_time = ohlc_cache[cache_key]
        age_seconds = (current_time - cached_time).total_seconds()
        
        if age_seconds < CACHE_DURATION_SECONDS:
            print(f"💾 Utilisation du cache pour {pair} (âge: {int(age_seconds)}s)")
            return cached_data
    
    # Sinon, récupérer depuis l'API
    print(f"🌐 Appel API pour {pair}...")
    df = fetch_ohlc_td(pair, interval, outputsize)
    
    # Mettre en cache
    ohlc_cache[cache_key] = (df, current_time)
    
    return df

def persist_signal(payload):
    q = text("INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json) "
             "VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload)")
    with engine.begin() as conn:
        conn.execute(q, payload)

def generate_daily_schedule_for_today():
    """
    Génère le planning des signaux du jour
    - Premier signal : envoi 9h00, entrée 9h03
    - Signaux toutes les 5 minutes
    - Total : 20 signaux par jour
    """
    today = datetime.utcnow().date()
    
    # Heure du premier envoi : 9h00 UTC
    first_send_time = datetime.combine(today, dtime(START_HOUR_UTC, 0, 0), tzinfo=timezone.utc)
    
    schedule = []
    active_pairs = PAIRS[:2]  # 2 paires pour respecter limite API
    
    for i in range(NUM_SIGNALS_PER_DAY):
        # Temps d'envoi du signal
        send_time = first_send_time + timedelta(minutes=i * SIGNAL_INTERVAL_MIN)
        
        # Temps d'entrée = temps d'envoi + délai de 3 minutes
        entry_time = send_time + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)
        
        # Alterner les paires
        pair = active_pairs[i % len(active_pairs)]
        
        schedule.append({
            'pair': pair,
            'send_time': send_time,
            'entry_time': entry_time
        })
    
    # Afficher le résumé
    first_signal = schedule[0]
    last_signal = schedule[-1]
    
    print(f"📅 Planning généré pour {today.strftime('%Y-%m-%d')}:")
    print(f"   • Nombre de signaux: {NUM_SIGNALS_PER_DAY}")
    print(f"   • Premier signal: Envoi {first_signal['send_time'].strftime('%H:%M')}, Entrée {first_signal['entry_time'].strftime('%H:%M')}")
    print(f"   • Dernier signal: Envoi {last_signal['send_time'].strftime('%H:%M')}, Entrée {last_signal['entry_time'].strftime('%H:%M')}")
    print(f"   • Paires actives: {', '.join(active_pairs)}")
    
    return schedule

def format_signal_message(pair, direction, entry_time, confidence, reason):
    """Formate le message de signal à envoyer"""
    direction_text = "BUY" if direction == "CALL" else "SELL"
    
    gale1 = entry_time + timedelta(minutes=5)
    gale2 = entry_time + timedelta(minutes=10)
    
    date_str = entry_time.strftime('%Y-%m-%d')
    time_str = entry_time.strftime('%H:%M')
    gale1_str = gale1.strftime('%H:%M')
    gale2_str = gale2.strftime('%H:%M')
    
    msg = (
        f"📊 SIGNAL — {pair} - {date_str}\n\n"
        f"Entrée (UTC): {time_str}\n\n"
        f"Direction: {direction_text}\n\n"
        f"     Gale 1: {gale1_str}\n"
        f"     Gale 2: {gale2_str}\n\n"
        f"Confiance: {int(confidence*100)}%"
    )
    return msg

# --- Commandes Telegram ---

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    print(f"📥 /start reçu de user_id={user_id} username={username}")
    try:
        with engine.begin() as conn:
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
                    f"📊 {NUM_SIGNALS_PER_DAY} signaux par jour (≥70% confiance)\n"
                    f"⏰ Premier signal à {START_HOUR_UTC}h00 UTC\n"
                    f"🔄 Un signal toutes les {SIGNAL_INTERVAL_MIN} minutes\n"
                    f"⏱️ Entrée {DELAY_BEFORE_ENTRY_MIN} minutes après l'envoi\n\n"
                    "Commandes:\n"
                    "/test - Tester un signal maintenant\n"
                    "/stats - Voir les statistiques\n"
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

async def cmd_train(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entraîne le modèle ML sur l'historique"""
    await update.message.reply_text("🎓 Entraînement du modèle ML en cours...")
    
    success = ml_predictor.train_on_history(engine)
    
    if success:
        await update.message.reply_text("✅ Modèle ML entraîné avec succès!")
    else:
        await update.message.reply_text("⚠️ Pas assez de données pour l'entraînement (minimum 50 signaux avec résultats)")

async def cmd_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Force la vérification des signaux en attente"""
    await update.message.reply_text("🔍 Vérification des signaux en cours...")
    
    # Ajouter l'utilisateur comme admin pour recevoir les rapports
    auto_verifier.add_admin(update.effective_chat.id)
    
    await auto_verifier.verify_pending_signals()
    
    await update.message.reply_text("✅ Vérification terminée!")

async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Teste la génération de signal immédiatement"""
    await update.message.reply_text("🔍 Test de génération de signal en cours...")
    
    pair = PAIRS[0]
    # Entrée dans 3 minutes comme en production
    entry_time = datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)
    
    await send_pre_signal(pair, entry_time, context.application)
    
    await update.message.reply_text(
        f"✅ Test terminé pour {pair}!\n"
        f"Entrée prévue: {entry_time.strftime('%H:%M:%S')} UTC"
    )

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Affiche les statistiques avec performance ML"""
    with engine.connect() as conn:
        total = conn.execute(text('SELECT COUNT(*) FROM signals')).scalar()
        wins = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result='WIN'")).scalar()
        losses = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result='LOSE'")).scalar()
        subs = conn.execute(text('SELECT COUNT(*) FROM subscribers')).scalar()
        
        verified = wins + losses
    
    winrate = (wins/verified*100) if verified > 0 else 0
    
    # Stats de performance ML
    perf_stats = auto_verifier.get_performance_stats() if auto_verifier else None
    
    msg = f"📊 **Statistiques Globales**\n\n"
    msg += f"Total signaux: {total}\n"
    msg += f"Vérifiés: {verified}\n"
    msg += f"✅ Victoires: {wins}\n"
    msg += f"❌ Défaites: {losses}\n"
    msg += f"📈 Taux de réussite: {winrate:.1f}%\n"
    msg += f"👥 Abonnés: {subs}\n"
    
    if perf_stats:
        msg += f"\n🤖 **Performance ML**\n"
        msg += f"Win rate: {perf_stats['winrate']:.1f}%\n"
        msg += f"Confiance moyenne: {perf_stats['avg_confidence']:.1%}\n"
    
    await update.message.reply_text(msg)

# --- Envoi de signaux ---

async def send_pre_signal(pair, entry_time, app):
    """Génère et envoie un signal"""
    now = datetime.utcnow()
    print(f"\n{'='*60}")
    print(f"🔄 GÉNÉRATION SIGNAL - {now.strftime('%H:%M:%S')} UTC")
    print(f"   Paire: {pair}")
    print(f"   Entrée prévue: {entry_time.strftime('%H:%M:%S')} UTC")
    print(f"{'='*60}")
    
    try:
        params = BEST_PARAMS.get(pair, {})
        ema_f = params.get('ema_fast', 8)
        ema_s = params.get('ema_slow', 21)
        rsi_l = params.get('rsi', 14)
        bb_l = params.get('bb', 20)

        print(f"📊 Récupération des données pour {pair}...")
        df = get_cached_ohlc(pair, TIMEFRAME_M1, outputsize=400)
        print(f"✅ {len(df)} bougies disponibles")
        
        df = compute_indicators(df, ema_fast=ema_f, ema_slow=ema_s, rsi_len=rsi_l, bb_len=bb_l)
        base_signal = rule_signal(df)
        
        if base_signal:
            # 🤖 VALIDATION ML
            print(f"🤖 Validation ML du signal {base_signal}...")
            ml_signal, ml_confidence = ml_predictor.predict_signal(df, base_signal)
            
            if ml_signal is None:
                print(f"❌ ML rejette le signal (confiance trop faible: {ml_confidence:.1%})")
                return
            
            if ml_confidence < 0.70:
                print(f"⚠️  Confiance ML insuffisante: {ml_confidence:.1%} (minimum 70%)")
                return
            
            direction = ml_signal
            confidence = ml_confidence
            reason = f'Signal ML validé: {int(confidence*100)}% confiance'
            print(f"✅ SIGNAL ML VALIDÉ: {direction} avec {int(confidence*100)}% confiance")
        else:
            print(f"⏭️  Pas de signal base pour {pair}")
            return

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
        print(f"💾 Signal sauvegardé en DB")

        with engine.connect() as conn:
            user_ids = [row[0] for row in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]

        if not user_ids:
            print("⚠️  Aucun abonné")
            return

        msg = format_signal_message(pair, direction, entry_time, confidence, reason)

        sent_count = 0
        failed_count = 0
        for uid in user_ids:
            try:
                await app.bot.send_message(chat_id=uid, text=msg)
                sent_count += 1
            except Exception as e:
                failed_count += 1
                print(f"❌ Erreur envoi à {uid}: {e}")

        print(f"\n{'='*60}")
        print(f"✅ SIGNAL ENVOYÉ")
        print(f"   Direction: {direction}")
        print(f"   Confiance: {int(confidence*100)}%")
        print(f"   Envoyé à: {sent_count}/{len(user_ids)} utilisateurs")
        if failed_count > 0:
            print(f"   ⚠️  Échecs: {failed_count}")
        print(f"   Entrée: {entry_time.strftime('%H:%M:%S')} UTC")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f'❌ ERREUR: {e}')
        import traceback
        traceback.print_exc()

# --- Scheduler ---

async def schedule_today_signals(app, sched):
    """Planifie tous les signaux du jour"""
    if datetime.utcnow().weekday() > 4:
        print('🏖️  Weekend - Pas de signaux planifiés')
        return

    # Supprimer les anciens jobs de signaux (garder les jobs récurrents)
    for job in sched.get_jobs():
        if job.id and job.id.startswith('signal_'):
            job.remove()
    
    daily = generate_daily_schedule_for_today()
    
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    scheduled_count = 0
    
    for item in daily:
        send_time = item['send_time']
        
        # Ne planifier que les signaux futurs
        if send_time > now:
            sched.add_job(
                send_pre_signal,
                'date',
                run_date=send_time,
                args=[item['pair'], item['entry_time'], app],
                id=f"signal_{item['pair']}_{send_time.strftime('%H%M')}"
            )
            scheduled_count += 1
    
    print(f"\n✅ {scheduled_count}/{len(daily)} signaux planifiés pour aujourd'hui")
    if scheduled_count > 0:
        next_signal = min([j.next_run_time for j in sched.get_jobs() if j.id and j.id.startswith('signal_')])
        print(f"   Prochain signal: {next_signal.strftime('%H:%M:%S')} UTC\n")

async def send_all_signals_now(app):
    """Pour test manuel uniquement"""
    print("🚀 Test signaux immédiat...")
    daily = generate_daily_schedule_for_today()
    
    for i, item in enumerate(daily[:3], 1):  # Tester seulement 3
        print(f"📤 Test {i}/3 pour {item['pair']}...")
        await send_pre_signal(item['pair'], item['entry_time'], app)
        if i < 3:
            await asyncio.sleep(10)
    
    print("✅ Tests terminés")

# --- DB ---

def ensure_db():
    sql = open('db_schema.sql').read()
    with engine.begin() as conn:
        for stmt in sql.split(';'):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

# --- Main ---

async def main():
    global auto_verifier
    
    print("\n" + "="*60)
    print("🤖 BOT DE TRADING ML - DÉMARRAGE")
    print("="*60)
    print(f"⏰ Configuration:")
    print(f"   • Premier signal: {START_HOUR_UTC}h00 UTC")
    print(f"   • Intervalle: {SIGNAL_INTERVAL_MIN} minutes")
    print(f"   • Délai entrée: {DELAY_BEFORE_ENTRY_MIN} minutes")
    print(f"   • Signaux/jour: {NUM_SIGNALS_PER_DAY}")
    print("="*60 + "\n")
    
    ensure_db()
    print("✅ Base de données initialisée")
    
    # Initialiser l'auto-verifier
    auto_verifier = AutoResultVerifier(engine, TWELVEDATA_API_KEY)
    print("✅ Vérificateur automatique initialisé")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('result', cmd_result))
    app.add_handler(CommandHandler('stats', cmd_stats))
    app.add_handler(CommandHandler('test', cmd_test))
    app.add_handler(CommandHandler('train', cmd_train))
    app.add_handler(CommandHandler('verify', cmd_verify))

    sched.start()
    print("✅ Scheduler démarré")
    
    # Planifier les signaux d'aujourd'hui
    await schedule_today_signals(app, sched)
    
    # Job quotidien pour planifier les signaux à 8h55 UTC (avant le premier signal de 9h)
    sched.add_job(
        schedule_today_signals,
        'cron',
        hour=8,
        minute=55,
        args=[app, sched],
        id='daily_schedule'
    )
    
    # 🤖 Job de vérification automatique toutes les 15 minutes
    sched.add_job(
        auto_verifier.verify_pending_signals,
        'interval',
        minutes=15,
        id='auto_verify'
    )
    
    print("\n📆 Jobs planifiés:")
    print("   • Planification quotidienne: 8h55 UTC")
    print("   • Vérification auto: Toutes les 15 min")

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)
    
    bot_info = await app.bot.get_me()
    
    print("\n" + "="*60)
    print("✅ BOT DÉMARRÉ ET OPÉRATIONNEL")
    print("="*60)
    print(f"🤖 Bot: @{bot_info.username}")
    print(f"🎓 Modèle ML: Actif")
    print(f"🔍 Vérification auto: Toutes les 15 min")
    print(f"⏰ Prochain signal: {START_HOUR_UTC}h00 UTC")
    print("="*60 + "\n")
    
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("\n" + "="*60)
        print("🛑 ARRÊT DU BOT")
        print("="*60)
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        sched.shutdown()
        print("✅ Bot arrêté proprement\n")

if __name__=='__main__':
    asyncio.run(main())
