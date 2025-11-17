"""
Bot de trading - Version corrigée
- Bloque le week-end
- Nettoie les signaux invalides
- Gère la limite API
- Vérifie la session 9h AM
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
from utils import compute_indicators, rule_signal
from ml_predictor import MLSignalPredictor
from auto_verifier import AutoResultVerifier

# Configuration
HAITI_TZ = ZoneInfo("America/Port-au-Prince")
START_HOUR_HAITI = 9
DELAY_BEFORE_ENTRY_MIN = 3
VERIFICATION_WAIT_MIN = 15
NUM_SIGNALS_PER_DAY = 20

engine = create_engine(DB_URL, connect_args={'check_same_thread': False})
sched = AsyncIOScheduler(timezone=HAITI_TZ)
ml_predictor = MLSignalPredictor()
auto_verifier = None
signal_queue_running = False

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
    """Vérifie si le marché Forex est ouvert"""
    now_utc = get_utc_now()
    weekday = now_utc.weekday()  # 0=lundi, 6=dimanche
    hour = now_utc.hour
    
    # Samedi : toujours fermé
    if weekday == 5:
        return False
    
    # Dimanche : ouvert après 22h UTC
    if weekday == 6 and hour < 22:
        return False
    
    # Vendredi : fermé après 22h UTC
    if weekday == 4 and hour >= 22:
        return False
    
    return True

def fetch_ohlc_td(pair, interval, outputsize=300):
    if not is_forex_open():
        raise RuntimeError("Marché Forex fermé")
    
    params = {'symbol': pair, 'interval': interval, 'outputsize': outputsize,
    'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TWELVE_TS_URL, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    
    # Vérifier limite API
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
    q = text("""INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json)
    VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload)""")
    with engine.begin() as conn:
        result = conn.execute(q, payload)
    return result.lastrowid

def cleanup_weekend_signals():
    """Supprime les signaux créés pendant le week-end"""
    try:
        with engine.begin() as conn:
            # Marquer comme LOSE les signaux du week-end sans résultat
            result = conn.execute(text("""
                UPDATE signals 
                SET result = 'LOSE', 
                    reason = 'Signal créé pendant week-end (marché fermé)'
                WHERE result IS NULL 
                AND (
                    CAST(strftime('%w', ts_enter) AS INTEGER) = 0 OR  -- Dimanche
                    CAST(strftime('%w', ts_enter) AS INTEGER) = 6     -- Samedi
                )
            """))
            
            count = result.rowcount
            if count > 0:
                print(f"🧹 {count} signaux du week-end nettoyés")
    except Exception as e:
        print(f"⚠️ Erreur cleanup: {e}")

def ensure_db():
    """Crée/met à jour la base de données"""
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
                conn.execute(text("ALTER TABLE signals ADD COLUMN max_gales INTEGER DEFAULT 2"))    
            
            if 'winning_attempt' not in existing_cols:    
                conn.execute(text("ALTER TABLE signals ADD COLUMN winning_attempt TEXT"))    
            
            print("✅ Base de données prête")
        
        # Nettoyer les signaux du week-end
        cleanup_weekend_signals()

    except Exception as e:
        print(f"⚠️ Erreur DB: {e}")

# === COMMANDES TELEGRAM ===

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    try:
        with engine.begin() as conn:
            existing = conn.execute(text("SELECT user_id FROM subscribers WHERE user_id = :uid"),
            {"uid": user_id}).fetchone()
            if existing:
                await update.message.reply_text("✅ Vous êtes déjà abonné aux signaux !")
            else:
                conn.execute(text("INSERT INTO subscribers (user_id, username) VALUES (:uid, :uname)"),
                {"uid": user_id, "uname": username})
                await update.message.reply_text(
                    f"✅ Bienvenue !\n\n"
                    f"📊 Jusqu'à {NUM_SIGNALS_PER_DAY} signaux/jour\n"
                    f"⏰ Début: {START_HOUR_HAITI}h00 AM (Haïti)\n"
                    f"🔄 Lundi-Vendredi (marché Forex)\n\n"
                    f"Commandes:\n"
                    f"/stats - Statistiques\n"
                    f"/verify - Vérifier signaux\n"
                    f"/status - État du bot\n"
                    f"/clean - Nettoyer week-end"
                )
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with engine.connect() as conn:
            total = conn.execute(text('SELECT COUNT(*) FROM signals')).scalar()
            wins = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result='WIN'")).scalar()
            losses = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result='LOSE'")).scalar()
            pending = conn.execute(text("SELECT COUNT(*) FROM signals WHERE result IS NULL")).scalar()
            subs = conn.execute(text('SELECT COUNT(*) FROM subscribers')).scalar()

        verified = wins + losses
        winrate = (wins/verified*100) if verified > 0 else 0

        msg = f"📊 **Statistiques**\n\n"    
        msg += f"Total signaux: {total}\n"    
        msg += f"Vérifiés: {verified}\n"    
        msg += f"✅ Réussis: {wins}\n"    
        msg += f"❌ Échoués: {losses}\n"    
        msg += f"⏳ En attente: {pending}\n"    
        msg += f"📈 Win rate: {winrate:.1f}%\n"    
        msg += f"👥 Abonnés: {subs}"    
        
        await update.message.reply_text(msg)

    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """État du système"""
    try:
        now_haiti = get_haiti_now()
        now_utc = get_utc_now()
        forex_open = is_forex_open()
        
        msg = f"🤖 **État du Bot**\n\n"
        msg += f"🇭🇹 Haïti: {now_haiti.strftime('%a %H:%M:%S')}\n"
        msg += f"🌍 UTC: {now_utc.strftime('%a %H:%M:%S')}\n"
        msg += f"📈 Forex: {'🟢 OUVERT' if forex_open else '🔴 FERMÉ'}\n"
        msg += f"🔄 Session: {'✅ Active' if signal_queue_running else '⏸️ Inactive'}\n\n"
        
        if not forex_open:
            if now_utc.weekday() == 6 and now_utc.hour < 22:
                msg += "⏰ Réouverture: Dimanche 22h UTC\n"
            elif now_utc.weekday() == 5:
                msg += "⏰ Réouverture: Dimanche 22h UTC\n"
            else:
                msg += "⏰ Réouverture: Lundi 00h UTC\n"
        
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_clean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Nettoie les signaux du week-end"""
    try:
        msg = await update.message.reply_text("🧹 Nettoyage en cours...")
        cleanup_weekend_signals()
        await msg.edit_text("✅ Signaux du week-end nettoyés!")
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    
    if not is_forex_open():
        await update.message.reply_text("⚠️ Marché Forex fermé. Vérification impossible.")
        return
    
    try:
        msg = await update.message.reply_text("🔍 Vérification en cours...")

        auto_verifier.add_admin(chat_id)
        if not auto_verifier.bot:
            auto_verifier.set_bot(context.application.bot)

        try:    
            await auto_verifier.verify_pending_signals()    
            await msg.edit_text("✅ Vérification terminée!")    
        except Exception as e:    
            await msg.edit_text(f"⚠️ Erreur: {str(e)[:100]}")

    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

# === ENVOI DE SIGNAUX ===

async def send_pre_signal(pair, entry_time_haiti, app):
    if not is_forex_open():
        print("🏖️ Marché fermé - Pas de signal")
        return None
    
    now_haiti = get_haiti_now()
    print(f"\n📤 Tentative signal {pair} - {now_haiti.strftime('%H:%M:%S')} (Haïti)")

    try:
        params = BEST_PARAMS.get(pair, {})
        df = get_cached_ohlc(pair, TIMEFRAME_M1, outputsize=400)

        if df is None or len(df) < 50:    
            print("❌ Pas de données disponibles")    
            return None    
                
        df = compute_indicators(df, ema_fast=params.get('ema_fast',8),    
                                ema_slow=params.get('ema_slow',21),    
                                rsi_len=params.get('rsi',14),    
                                bb_len=params.get('bb',20))    
        base_signal = rule_signal(df)    
            
        if not base_signal:    
            print("⏭️ Pas de signal de base")    
            return None    
            
        ml_signal, ml_conf = ml_predictor.predict_signal(df, base_signal)    
        if ml_signal is None or ml_conf < 0.70:    
            print(f"❌ Rejeté par ML ({ml_conf:.1%})")    
            return None    
            
        entry_time_utc = entry_time_haiti.astimezone(timezone.utc)    
            
        payload = {    
            'pair': pair, 'direction': ml_signal, 'reason': f'ML {ml_conf:.1%}',    
            'ts_enter': entry_time_utc.isoformat(), 'ts_send': get_utc_now().isoformat(),    
            'confidence': ml_conf, 'payload': json.dumps({'pair': pair})    
        }    
        signal_id = persist_signal(payload)    
            
        with engine.connect() as conn:    
            user_ids = [r[0] for r in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]    
            
        direction_text = "BUY" if ml_signal == "CALL" else "SELL"    
        gale1_haiti = entry_time_haiti + timedelta(minutes=5)    
        gale2_haiti = entry_time_haiti + timedelta(minutes=10)    
            
        msg = (    
            f"📊 SIGNAL — {pair}\n\n"    
            f"🕐 Entrée: {entry_time_haiti.strftime('%H:%M')} (Haïti)\n\n"    
            f"📈 Direction: {direction_text}\n\n"    
            f"🔄 Gale 1: {gale1_haiti.strftime('%H:%M')}\n"    
            f"🔄 Gale 2: {gale2_haiti.strftime('%H:%M')}\n\n"    
            f"💪 Confiance: {int(ml_conf*100)}%"    
        )    
            
        for uid in user_ids:    
            try:    
                await app.bot.send_message(chat_id=uid, text=msg)    
            except Exception as e:    
                print(f"❌ Envoi à {uid}: {e}")    
            
        print(f"✅ Signal envoyé ({ml_signal}, {ml_conf:.1%})")    
        return signal_id

    except Exception as e:
        print(f"❌ Erreur signal: {e}")
        return None

async def send_verification_result(signal_id, app):
    try:
        with engine.connect() as conn:
            signal = conn.execute(
                text("SELECT pair, direction, result, gale_level FROM signals WHERE id = :sid"),
                {"sid": signal_id}
            ).fetchone()

        if not signal or not signal[2]:
            return

        pair, direction, result, gale_level = signal    
        user_ids = [r[0] for r in conn.execute(text("SELECT user_id FROM subscribers")).fetchall()]    
            
        if result == "WIN":    
            emoji, status = "✅", "GAGNÉ"    
            gale_names = ["Signal initial", "Gale 1", "Gale 2"]    
            gale_text = gale_names[gale_level] if gale_level < 3 else f"Gale {gale_level}"    
        else:    
            emoji, status, gale_text = "❌", "PERDU", "Après 3 tentatives"    
            
        msg = f"{emoji} {status}\n{pair} - {direction}\n{gale_text}"    
            
        for uid in user_ids:    
            try:    
                await app.bot.send_message(chat_id=uid, text=msg)    
            except:    
                pass    
            
        print(f"📤 Résultat envoyé: {status}")

    except Exception as e:
        print(f"❌ Erreur envoi résultat: {e}")

# === FILE DE SIGNAUX ===

async def process_signal_queue(app):
    global signal_queue_running

    if not is_forex_open():
        print("🏖️ Marché Forex fermé - Session annulée")
        return

    if signal_queue_running:
        print("⚠️ File déjà en cours")
        return

    signal_queue_running = True

    try:
        now_haiti = get_haiti_now()

        print(f"\n{'='*60}")    
        print(f"🚀 DÉBUT DE LA SESSION")    
        print(f"{'='*60}")    
        print(f"🕐 Heure Haïti: {now_haiti.strftime('%H:%M:%S')}")    
        print(f"🌍 Heure UTC: {get_utc_now().strftime('%H:%M:%S')}")    
        print(f"{'='*60}\n")    
            
        active_pairs = PAIRS[:2]    
            
        for i in range(NUM_SIGNALS_PER_DAY):    
            if not is_forex_open():
                print("🏖️ Marché fermé - Arrêt session")
                break
            
            pair = active_pairs[i % len(active_pairs)]    
                
            print(f"\n{'─'*60}")    
            print(f"📍 SIGNAL {i+1}/{NUM_SIGNALS_PER_DAY} - {pair}")    
            print(f"{'─'*60}")    
                
            now_haiti = get_haiti_now()    
            entry_time_haiti = now_haiti + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)    
                
            signal_id = None    
            for attempt in range(3):    
                signal_id = await send_pre_signal(pair, entry_time_haiti, app)    
                if signal_id:    
                    break    
                await asyncio.sleep(30)    
                
            if not signal_id:    
                print(f"❌ Aucun signal valide")    
                continue    
                
            verification_time_haiti = entry_time_haiti + timedelta(minutes=VERIFICATION_WAIT_MIN)    
            now_haiti = get_haiti_now()    
            wait_seconds = (verification_time_haiti - now_haiti).total_seconds()    
                
            if wait_seconds > 0:    
                print(f"⏳ Attente de {wait_seconds/60:.1f} min")    
                await asyncio.sleep(wait_seconds)    
                
            print(f"🔍 Vérification...")    
                
            try:    
                await auto_verifier.verify_pending_signals()    
            except:    
                pass    
                
            await send_verification_result(signal_id, app)    
                
            print(f"✅ Cycle {i+1} terminé\n")    
            await asyncio.sleep(30)    
            
        print(f"\n{'='*60}")    
        print(f"🏁 SESSION TERMINÉE")    
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        signal_queue_running = False

async def start_daily_signals(app):
    """Démarre la session quotidienne"""
    now_haiti = get_haiti_now()
    
    # Vérifier que c'est un jour de semaine
    if now_haiti.weekday() > 4:
        print("🏖️ Week-end - Pas de session")
        return
    
    # Vérifier que le marché est ouvert
    if not is_forex_open():
        print("🏖️ Marché fermé - Pas de session")
        return

    print(f"\n📅 Démarrage session - {now_haiti.strftime('%H:%M:%S')}")
    asyncio.create_task(process_signal_queue(app))

# === MAIN ===

async def main():
    global auto_verifier

    now_haiti = get_haiti_now()
    now_utc = get_utc_now()

    print("\n" + "="*60)
    print("🤖 BOT DE TRADING - HAÏTI")
    print("="*60)
    print(f"🇭🇹 Heure Haïti: {now_haiti.strftime('%H:%M:%S %Z')}")
    print(f"🌍 Heure UTC: {now_utc.strftime('%H:%M:%S %Z')}")
    print(f"📈 Marché Forex: {'🟢 OUVERT' if is_forex_open() else '🔴 FERMÉ'}")
    print(f"⏰ Début quotidien: {START_HOUR_HAITI}h00 AM (Haïti)")
    print(f"📊 Signaux: Lundi-Vendredi uniquement")
    print("="*60 + "\n")

    ensure_db()
    auto_verifier = AutoResultVerifier(engine, TWELVEDATA_API_KEY)

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('stats', cmd_stats))
    app.add_handler(CommandHandler('status', cmd_status))
    app.add_handler(CommandHandler('clean', cmd_clean))
    app.add_handler(CommandHandler('verify', cmd_verify))

    sched.start()

    # Vérification automatique - SEULEMENT SI MARCHÉ OUVERT
    async def conditional_verify():
        if is_forex_open():
            await auto_verifier.verify_pending_signals()
    
    sched.add_job(
        conditional_verify,
        'interval',
        minutes=5,
        id='auto_verify'
    )

    # Démarrer immédiatement si conditions OK
    if (now_haiti.hour >= START_HOUR_HAITI and now_haiti.hour < 18 and
        now_haiti.weekday() <= 4 and not signal_queue_running and is_forex_open()):
        print("🚀 Démarrage immédiat de la session")
        asyncio.create_task(process_signal_queue(app))

    # Planifier la session quotidienne
    sched.add_job(
        start_daily_signals,
        'cron',
        hour=START_HOUR_HAITI,
        minute=0,
        timezone=HAITI_TZ,
        args=[app],
        id='daily_signals_haiti'
    )

    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

    bot_info = await app.bot.get_me()
    print(f"✅ BOT ACTIF: @{bot_info.username}")
    print(f"📍 Prochaine session: {START_HOUR_HAITI}h00 AM (Haïti)\n")

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print("\n🛑 Arrêt du bot...")
        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        sched.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
