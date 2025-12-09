"""
Bot M5 avec Vérification Synchronisée
Attend vérification du signal précédent AVANT d'envoyer le suivant
"""

import os, json, asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import create_engine, text
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from config import *
from utils import compute_indicators, rule_signal_ultra_strict, get_signal_quality_score
from ml_predictor import MLSignalPredictor
from auto_verifier import AutoResultVerifier
from ml_continuous_learning import ContinuousLearning
from backtester import BacktesterM5

# Configuration
HAITI_TZ = ZoneInfo("America/Port-au-Prince")

SCHEDULED_SESSIONS = [
    {
        'name': 'London Kill Zone',
        'start_hour': 2,
        'start_minute': 0,
        'end_hour': 5,
        'end_minute': 0,
        'signals_count': 3,
        'interval_minutes': 30,
        'priority': 3,
        'wait_verification': True  # Attendre vérification
    },
    {
        'name': 'London/NY Overlap',
        'start_hour': 9,
        'start_minute': 0,
        'end_hour': 11,
        'end_minute': 0,
        'signals_count': 4,
        'interval_minutes': 30,
        'priority': 5,
        'wait_verification': True
    },
    {
        'name': 'NY Session',
        'start_hour': 14,
        'start_minute': 0,
        'end_hour': 17,
        'end_minute': 0,
        'signals_count': 4,
        'interval_minutes': 30,
        'priority': 3,
        'wait_verification': True
    },
    {
        'name': 'Evening Session',
        'start_hour': 18,
        'start_minute': 0,
        'end_hour': 2,
        'end_minute': 0,
        'signals_count': -1,
        'interval_minutes': 15,  # 15 min au lieu de 10 (temps pour vérification)
        'priority': 2,
        'continuous': True,
        'wait_verification': True
    }
]

# Paramètres
TIMEFRAME_M5 = "5min"
DELAY_BEFORE_ENTRY_MIN = 5
VERIFICATION_WAIT_MIN = 5
CONFIDENCE_THRESHOLD = 0.70  # Augmenté de 0.65 à 0.70

engine = create_engine(DB_URL, connect_args={'check_same_thread': False})
sched = AsyncIOScheduler(timezone=HAITI_TZ)
ml_predictor = MLSignalPredictor()
auto_verifier = None
active_sessions = {}
session_running = {}
last_signal_pending_verification = {}

BEST_PARAMS = {}
if os.path.exists(BEST_PARAMS_FILE):
    try:
        with open(BEST_PARAMS_FILE, 'r') as f:
            BEST_PARAMS = json.load(f)
    except:
        pass

TWELVE_TS_URL = 'https://api.twelvedata.com/time_series'
ohlc_cache = {}

def get_haiti_now():
    return datetime.now(HAITI_TZ)

def get_utc_now():
    return datetime.now(timezone.utc)

def is_forex_open():
    now_utc = get_utc_now()
    weekday = now_utc.weekday()
    hour = now_utc.hour
    
    if weekday == 5:
        return False
    if weekday == 6 and hour < 22:
        return False
    if weekday == 4 and hour >= 22:
        return False
    
    return True

def get_current_session():
    now_haiti = get_haiti_now()
    current_time = now_haiti.hour * 60 + now_haiti.minute
    
    for session in SCHEDULED_SESSIONS:
        start_time = session['start_hour'] * 60 + session['start_minute']
        end_time = session['end_hour'] * 60 + session['end_minute']
        
        if session.get('continuous') and session['end_hour'] < session['start_hour']:
            if current_time >= start_time or current_time < end_time:
                return session
        else:
            if start_time <= current_time < end_time:
                return session
    
    return None

def fetch_ohlc_td(pair, interval, outputsize=300):
    if not is_forex_open():
        raise RuntimeError("Marché Forex fermé")
    
    params = {'symbol': pair, 'interval': interval, 'outputsize': outputsize,
    'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TWELVE_TS_URL, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    
    if 'code' in j and j['code'] == 429:
        raise RuntimeError(f"Limite API atteinte: {j.get('message', 'Unknown')}")
    
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
    if not is_forex_open():
        return None
    
    cache_key = f"{pair}_{interval}"
    current_time = get_utc_now()
    
    if cache_key in ohlc_cache:
        cached_data, cached_time = ohlc_cache[cache_key]
        if (current_time - cached_time).total_seconds() < 60:
            return cached_data
    
    try:
        df = fetch_ohlc_td(pair, interval, outputsize)
        ohlc_cache[cache_key] = (df, current_time)
        return df
    except RuntimeError as e:
        print(f"⚠️ Cache OHLC: {e}")
        return None

def persist_signal(payload):
    q = text("""INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json,max_gales)
    VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload,:max_gales)""")
    with engine.begin() as conn:
        result = conn.execute(q, payload)
    return result.lastrowid

def ensure_db():
    try:
        sql = open('db_schema.sql').read()
        with engine.begin() as conn:
            for stmt in sql.split(';'):
                if stmt.strip():
                    conn.execute(text(stmt.strip()))

        with engine.begin() as conn:
            result = conn.execute(text("PRAGMA table_info(signals)")).fetchall()
            existing_cols = {row[1] for row in result}
            
            if 'gale_level' not in existing_cols:
                conn.execute(text("ALTER TABLE signals ADD COLUMN gale_level INTEGER DEFAULT 0"))
            if 'timeframe' not in existing_cols:
                conn.execute(text("ALTER TABLE signals ADD COLUMN timeframe INTEGER DEFAULT 5"))
            if 'max_gales' not in existing_cols:
                conn.execute(text("ALTER TABLE signals ADD COLUMN max_gales INTEGER DEFAULT 0"))
            if 'winning_attempt' not in existing_cols:
                conn.execute(text("ALTER TABLE signals ADD COLUMN winning_attempt TEXT"))
            if 'reason' not in existing_cols:
                conn.execute(text("ALTER TABLE signals ADD COLUMN reason TEXT"))
            if 'kill_zone' not in existing_cols:
                conn.execute(text("ALTER TABLE signals ADD COLUMN kill_zone TEXT"))
            
            print("✅ Base de données prête")

    except Exception as e:
        print(f"⚠️ Erreur DB: {e}")

# [Les commandes Telegram sont identiques, je les garde pour référence mais ne les réécris pas toutes]

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... [Identique au bot précédent]
    pass

# ... [Autres commandes identiques] ...

async def send_single_signal(app, session):
    """Envoie un signal avec vérification STRICTE"""
    try:
        if not is_forex_open():
            print("[SIGNAL] 🏖️ Marché fermé")
            return None
        
        now_haiti = get_haiti_now()
        print(f"\n[SIGNAL] 📤 {session['name']} - {now_haiti.strftime('%H:%M:%S')}")
        
        # Rotation paires
        active_pairs = PAIRS[:3]
        session_signals = active_sessions.get(session['name'], [])
        pair = active_pairs[len(session_signals) % len(active_pairs)]
        
        print(f"[SIGNAL] 🔍 Analyse {pair}...")
        
        # Récupérer données
        params = BEST_PARAMS.get(pair, {})
        df = get_cached_ohlc(pair, TIMEFRAME_M5, outputsize=400)
        
        if df is None or len(df) < 50:
            print("[SIGNAL] ❌ Pas assez de données")
            return None
        
        print(f"[SIGNAL] ✅ {len(df)} bougies chargées")
        
        # Calculer indicateurs
        df = compute_indicators(df, ema_fast=params.get('ema_fast',8),
                                ema_slow=params.get('ema_slow',21),
                                rsi_len=params.get('rsi',14),
                                bb_len=params.get('bb',20))
        
        print(f"[SIGNAL] ✅ Indicateurs calculés")
        
        # Analyser avec stratégie ULTRA-STRICTE
        base_signal = rule_signal_ultra_strict(df, session_priority=session['priority'])
        
        if not base_signal:
            print("[SIGNAL] ⏭️ Rejeté par stratégie STRICTE")
            last = df.iloc[-1]
            print(f"[DEBUG] ADX: {last.get('adx', 0):.1f}")
            print(f"[DEBUG] RSI: {last.get('rsi', 0):.1f}")
            print(f"[DEBUG] Momentum 3: {last.get('momentum_3', 0):.4f}")
            print(f"[DEBUG] Momentum 5: {last.get('momentum_5', 0):.4f}")
            return None
        
        print(f"[SIGNAL] ✅ Signal stratégie: {base_signal}")
        
        # Score qualité
        quality_score = get_signal_quality_score(df)
        print(f"[SIGNAL] 📊 Score qualité: {quality_score}/100")
        
        # Rejeter si score trop faible
        if quality_score < 70:
            print(f"[SIGNAL] ❌ Score insuffisant ({quality_score} < 70)")
            return None
        
        # ML prediction avec seuil augmenté
        ml_signal, ml_conf = ml_predictor.predict_signal(df, base_signal)
        if ml_signal is None or ml_conf < CONFIDENCE_THRESHOLD:
            print(f"[SIGNAL] ❌ Rejeté par ML ({ml_conf:.1%} < {CONFIDENCE_THRESHOLD:.0%})")
            return None
        
        print(f"[SIGNAL] ✅ ML approved: {ml_signal} ({ml_conf:.1%})")
        
        # Persister signal
        entry_time_haiti = now_haiti + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)
        entry_time_utc = entry_time_haiti.astimezone(timezone.utc)
        
        payload = {
            'pair': pair, 'direction': ml_signal, 
            'reason': f'ML {ml_conf:.1%} Q{quality_score} - {session["name"]}',
            'ts_enter': entry_time_utc.isoformat(), 
            'ts_send': get_utc_now().isoformat(),
            'confidence': ml_conf, 
            'payload': json.dumps({'pair': pair, 'session': session['name'], 'quality': quality_score}),
            'max_gales': 0
        }
        signal_id = persist_signal(payload)
        
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE signals SET kill_zone = :kz WHERE id = :sid"),
                    {'kz': session['name'], 'sid': signal_id}
                )
        except:
            pass
        
        # Envoyer aux abonnés
        with engine.connect() as conn:
            user_ids = [r[0] for r in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]
        
        direction_text = "BUY" if ml_signal == "CALL" else "SELL"
        
        msg = (
            f"🎯 SIGNAL — {pair}\n\n"
            f"📅 Session: {session['name']}\n"
            f"🔥 Priorité: {session['priority']}/5\n"
            f"🕐 Entrée: {entry_time_haiti.strftime('%H:%M')} (Haïti)\n"
            f"📍 Timeframe: M5\n\n"
            f"📈 Direction: **{direction_text}**\n"
            f"💪 Confiance: **{int(ml_conf*100)}%**\n"
            f"⭐ Qualité: **{quality_score}/100**\n\n"
            f"🔍 Vérif: 5 min après entrée"
        )
        
        sent_count = 0
        for uid in user_ids:
            try:
                await app.bot.send_message(chat_id=uid, text=msg)
                sent_count += 1
            except Exception as e:
                print(f"[SIGNAL] ❌ Envoi à {uid}: {e}")
        
        print(f"[SIGNAL] ✅ Envoyé à {sent_count} abonnés (ID: {signal_id})")
        
        # Tracking
        if session['name'] not in active_sessions:
            active_sessions[session['name']] = []
        active_sessions[session['name']].append(signal_id)
        
        # Marquer pour vérification
        last_signal_pending_verification[session['name']] = {
            'signal_id': signal_id,
            'entry_time': entry_time_utc,
            'verification_time': entry_time_utc + timedelta(minutes=VERIFICATION_WAIT_MIN)
        }
        
        return signal_id
        
    except Exception as e:
        print(f"[SIGNAL] ❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
        return None

async def wait_and_verify_signal(app, signal_id, verification_time):
    """Attend puis vérifie un signal"""
    try:
        now = get_utc_now()
        wait_seconds = (verification_time - now).total_seconds()
        
        if wait_seconds > 0:
            print(f"[VERIF] ⏳ Attente {int(wait_seconds)}s avant vérification signal #{signal_id}")
            await asyncio.sleep(wait_seconds)
        
        print(f"[VERIF] 🔍 Vérification signal #{signal_id}")
        
        # Vérifier via auto_verifier
        verified = await auto_verifier.verify_single_signal(signal_id)
        
        if verified:
            # Récupérer résultat
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT pair, direction, result FROM signals WHERE id = :sid"),
                    {'sid': signal_id}
                ).fetchone()
            
            if result and result[2]:
                pair, direction, outcome = result
                emoji = "✅" if outcome == "WIN" else "❌"
                status = "GAGNÉ" if outcome == "WIN" else "PERDU"
                
                print(f"[VERIF] {emoji} Signal #{signal_id}: {status}")
                
                # Envoyer briefing
                with engine.connect() as conn:
                    user_ids = [r[0] for r in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]
                
                briefing = (
                    f"{emoji} **RÉSULTAT**\n\n"
                    f"📊 {pair} {direction}\n"
                    f"🎲 {status}\n"
                )
                
                for uid in user_ids:
                    try:
                        await app.bot.send_message(chat_id=uid, text=briefing)
                    except:
                        pass
                
                return outcome == "WIN"
        
        return False
        
    except Exception as e:
        print(f"[VERIF] ❌ Erreur: {e}")
        return False

async def run_scheduled_session(app, session):
    """Exécute une session avec vérification synchronisée"""
    if not is_forex_open():
        print(f"[SESSION] 🏖️ Marché fermé - {session['name']}")
        return
    
    if session_running.get(session['name'], False):
        print(f"[SESSION] ⚠️ {session['name']} déjà en cours")
        return
    
    session_running[session['name']] = True
    
    print(f"\n[SESSION] 🚀 DÉBUT - {session['name']}")
    print(f"[SESSION] 🔥 Priorité: {session['priority']}/5")
    print(f"[SESSION] ⏰ Vérification: {'ACTIVE' if session.get('wait_verification') else 'PASSIVE'}")
    
    active_sessions[session['name']] = []
    
    try:
        if session.get('continuous'):
            # Mode continu avec vérification
            print(f"[SESSION] ⚡ Mode INTENSIF - {session['interval_minutes']}min jusqu'à {session['end_hour']:02d}h")
            
            signal_count = 0
            while True:
                current_session = get_current_session()
                if not current_session or current_session['name'] != session['name']:
                    print(f"[SESSION] ⏰ Fin de session")
                    break
                
                if not is_forex_open():
                    print(f"[SESSION] 🏖️ Marché fermé")
                    break
                
                signal_count += 1
                print(f"\n[SESSION] 📍 Signal #{signal_count}")
                
                # Envoyer signal
                signal_id = await send_single_signal(app, session)
                
                if signal_id:
                    print(f"[SESSION] ✅ Signal #{signal_count} envoyé (ID: {signal_id})")
                    
                    # Attendre vérification si activée
                    if session.get('wait_verification'):
                        pending = last_signal_pending_verification.get(session['name'])
                        if pending:
                            print(f"[SESSION] ⏳ Attente vérification signal #{signal_id}...")
                            await wait_and_verify_signal(app, signal_id, pending['verification_time'])
                            print(f"[SESSION] ✅ Vérification terminée")
                    
                else:
                    print(f"[SESSION] ⏭️ Signal #{signal_count} non généré")
                
                # Attendre intervalle
                print(f"[SESSION] ⏸️ Pause {session['interval_minutes']}min...")
                await asyncio.sleep(session['interval_minutes'] * 60)
            
            signals_sent = len(active_sessions.get(session['name'], []))
            print(f"\n[SESSION] 🏁 FIN - {session['name']}")
            print(f"[SESSION] 📊 {signals_sent} signaux envoyés")
            
        else:
            # Mode standard avec vérification
            print(f"[SESSION] ⚡ {session['signals_count']} signaux à {session['interval_minutes']}min")
            
            for i in range(session['signals_count']):
                if not is_forex_open():
                    print(f"[SESSION] 🏖️ Marché fermé")
                    break
                
                print(f"\n[SESSION] 📍 Signal {i+1}/{session['signals_count']}")
                
                # 3 tentatives
                signal_sent = False
                signal_id = None
                for attempt in range(3):
                    signal_id = await send_single_signal(app, session)
                    if signal_id:
                        signal_sent = True
                        break
                    
                    if attempt < 2:
                        print(f"[SESSION] ⏳ Attente 20s...")
                        await asyncio.sleep(20)
                
                if signal_sent and signal_id:
                    # Attendre vérification si activée
                    if session.get('wait_verification'):
                        pending = last_signal_pending_verification.get(session['name'])
                        if pending:
                            print(f"[SESSION] ⏳ Attente vérification signal #{signal_id}...")
                            await wait_and_verify_signal(app, signal_id, pending['verification_time'])
                            print(f"[SESSION] ✅ Vérification terminée")
                else:
                    print(f"[SESSION] ⚠️ Signal {i+1} non envoyé")
                
                # Attendre intervalle
                if i < session['signals_count'] - 1:
                    print(f"[SESSION] ⏸️ Pause {session['interval_minutes']}min...")
                    await asyncio.sleep(session['interval_minutes'] * 60)
            
            signals_sent = len(active_sessions.get(session['name'], []))
            print(f"\n[SESSION] 🏁 FIN - {session['name']}")
            print(f"[SESSION] 📊 {signals_sent}/{session['signals_count']} envoyés")
    
    finally:
        session_running[session['name']] = False

# [Le reste du code main() est identique]

async def main():
    global auto_verifier

    now_haiti = get_haiti_now()
    
    print("\n" + "="*60)
    print("🤖 BOT M5 - VÉRIFICATION SYNCHRONISÉE")
    print("="*60)
    print(f"📍 Seuil ML: {CONFIDENCE_THRESHOLD:.0%}")
    print(f"⏰ Evening: 15min (temps pour vérif)")
    print(f"🔍 Vérif AVANT signal suivant: ACTIVÉE")
    print("="*60 + "\n")

    ensure_db()
    auto_verifier = AutoResultVerifier(engine, TWELVEDATA_API_KEY)

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    # [Ajouter handlers...]
    
    sched.start()

    # [Planifier sessions...]
    for session in SCHEDULED_SESSIONS:
        job_id = f"session_{session['name'].lower().replace(' ', '_').replace('/', '_')}"
        sched.add_job(
            run_scheduled_session,
            'cron',
            hour=session['start_hour'],
            minute=session['start_minute'],
            timezone=HAITI_TZ,
            args=[app, session],
            id=job_id
        )
    
    current_session = get_current_session()
    if current_session and is_forex_open():
        print(f"\n🚀 LANCEMENT IMMÉDIAT - {current_session['name']}")
        asyncio.create_task(run_scheduled_session(app, current_session))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    bot_info = await app.bot.get_me()
    print(f"✅ BOT ACTIF: @{bot_info.username}\n")

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        sched.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
