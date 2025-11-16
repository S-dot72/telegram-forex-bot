"""
Bot de trading - Signaux séquentiels après vérification

Démarre à 9h AM heure d'Haïti (UTC-5)

Envoie signal → attend vérification → envoie résultat → nouveau signal

20 signaux max par jour
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
HAITI_TZ = ZoneInfo("America/Port-au-Prince")  # UTC-5
START_HOUR_HAITI = 9  # 9h AM heure d'Haïti
DELAY_BEFORE_ENTRY_MIN = 3
VERIFICATION_WAIT_MIN = 15  # Attendre 15 min après entrée avant vérification
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
        if (current_time - cached_time).total_seconds() < 60:
            return cached_data
    df = fetch_ohlc_td(pair, interval, outputsize)
    ohlc_cache[cache_key] = (df, current_time)
    return df

def persist_signal(payload):
    q = text("""INSERT INTO signals (pair,direction,reason,ts_enter,ts_send,confidence,payload_json)
    VALUES (:pair,:direction,:reason,:ts_enter,:ts_send,:confidence,:payload)""")
    with engine.begin() as conn:
        result = conn.execute(q, payload)
    return result.lastrowid

def ensure_db():
    """Crée/met à jour la base de données"""
    try:
        sql = open('db_schema.sql').read()
        with engine.begin() as conn:
            for stmt in sql.split(';'):
                if stmt.strip():
                    conn.execute(text(stmt.strip()))

        # FORCER l'ajout des colonnes manquantes
        with engine.begin() as conn:    
            # Vérifier quelles colonnes existent    
            result = conn.execute(text("PRAGMA table_info(signals)")).fetchall()    
            existing_cols = {row[1] for row in result}    
                
            print(f"📋 Colonnes existantes dans signals: {existing_cols}")    
                
            # Ajouter gale_level si manquante    
            if 'gale_level' not in existing_cols:    
                try:    
                    conn.execute(text("ALTER TABLE signals ADD COLUMN gale_level INTEGER DEFAULT 0"))    
                    print("✅ Colonne gale_level ajoutée")    
                except Exception as e:    
                    print(f"⚠️ gale_level: {e}")    
            
            # Ajouter timeframe si manquante    
            if 'timeframe' not in existing_cols:    
                try:    
                    conn.execute(text("ALTER TABLE signals ADD COLUMN timeframe INTEGER DEFAULT 5"))    
                    print("✅ Colonne timeframe ajoutée")    
                except Exception as e:    
                    print(f"⚠️ timeframe: {e}")    
            
            # Ajouter max_gales si manquante    
            if 'max_gales' not in existing_cols:    
                try:    
                    conn.execute(text("ALTER TABLE signals ADD COLUMN max_gales INTEGER DEFAULT 2"))    
                    print("✅ Colonne max_gales ajoutée")    
                except Exception as e:    
                    print(f"⚠️ max_gales: {e}")    
            
            # Ajouter winning_attempt si manquante    
            if 'winning_attempt' not in existing_cols:    
                try:    
                    conn.execute(text("ALTER TABLE signals ADD COLUMN winning_attempt TEXT"))    
                    print("✅ Colonne winning_attempt ajoutée")    
                except Exception as e:    
                    print(f"⚠️ winning_attempt: {e}")    
            
            print("✅ Structure de la table signals mise à jour")

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
                    f"🔄 Signal → Vérification → Résultat → Nouveau signal\n\n"
                    f"Commandes principales:\n"
                    f"/test - Tester un signal\n"
                    f"/stats - Voir les stats\n"
                    f"/verify - Vérifier tous les signaux\n\n"
                    f"Commandes avancées:\n"
                    f"/force - Forcer démarrage session\n"
                    f"/debug - Voir derniers signaux\n"
                    f"/check <id> - Vérifier un signal spécifique\n"
                    f"/debug_verify - Debug détaillé vérification"
                )
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        with engine.connect() as conn:
            total = conn.execute(text('SELECT COUNT() FROM signals')).scalar()
            wins = conn.execute(text("SELECT COUNT() FROM signals WHERE result='WIN'")).scalar()
            losses = conn.execute(text("SELECT COUNT() FROM signals WHERE result='LOSE'")).scalar()
            pending = conn.execute(text("SELECT COUNT() FROM signals WHERE result IS NULL")).scalar()
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

async def cmd_verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        msg = await update.message.reply_text("🔍 Vérification en cours...")

        auto_verifier.add_admin(chat_id)
        if not auto_verifier.bot:
            auto_verifier.set_bot(context.application.bot)

        try:    
            await auto_verifier.verify_pending_signals()    
            await msg.edit_text("✅ Vérification terminée! Consultez le rapport ci-dessus.")    
        except Exception as e:    
            print(f"❌ Erreur lors de la vérification: {e}")    
            import traceback    
            traceback.print_exc()    
            await msg.edit_text(f"⚠️ Erreur de vérification: {str(e)[:100]}")

    except Exception as e:
        print(f"❌ Erreur cmd_verify: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🔍 Test de signal...")
        pair = PAIRS[0]
        entry_time_haiti = get_haiti_now() + timedelta(minutes=DELAY_BEFORE_ENTRY_MIN)
        signal_id = await send_pre_signal(pair, entry_time_haiti, context.application)

        if signal_id:
            await update.message.reply_text(f"✅ Signal envoyé (ID: {signal_id})")
        else:
            await update.message.reply_text("❌ Pas de signal valide actuellement.")
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_force(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global signal_queue_running

    if signal_queue_running:
        await update.message.reply_text("⚠️ Une session est déjà en cours!")
        return

    try:
        await update.message.reply_text("🚀 Démarrage forcé de la session...")
        asyncio.create_task(process_signal_queue(context.application))
        await update.message.reply_text("✅ Session démarrée!")
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        now_utc = get_utc_now()
        now_haiti = get_haiti_now()

        with engine.connect() as conn:
            signals = conn.execute(
                text("SELECT id, pair, direction, result, gale_level, ts_enter FROM signals ORDER BY id DESC LIMIT 5")
            ).fetchall()

        if not signals:    
            await update.message.reply_text("Aucun signal en base")    
            return    
            
        msg = f"🔍 **Debug - Derniers signaux**\n\n"    
        msg += f"⏰ Maintenant UTC: {now_utc.strftime('%H:%M:%S')}\n"    
        msg += f"⏰ Maintenant Haïti: {now_haiti.strftime('%H:%M:%S')}\n"    
        msg += f"{'─'*30}\n\n"    
            
        for sig in signals:    
            sid, pair, direction, result, gale, ts_enter = sig    
            result_text = result if result else "⏳ En attente"    
            gale_text = f" (Gale {gale})" if gale else ""    
                
            try:    
                entry_time = datetime.fromisoformat(ts_enter.replace('Z', '+00:00'))    
            except:    
                entry_time = datetime.fromisoformat(ts_enter)    
                if entry_time.tzinfo is None:    
                    entry_time = entry_time.replace(tzinfo=timezone.utc)    
                
            end_time = entry_time + timedelta(minutes=15)    
            time_left = (end_time - now_utc).total_seconds() / 60    
                
            msg += f"**#{sid}** {pair} {direction}\n"    
            msg += f"📊 {result_text}{gale_text}\n"    
            msg += f"🕐 Entrée: {entry_time.strftime('%H:%M')} UTC\n"    
                
            if not result:    
                if time_left > 0:    
                    msg += f"⏳ Reste {time_left:.0f} min\n"    
                else:    
                    msg += f"✅ Prêt pour /verify\n"    
                
            msg += "\n"    
            
        await update.message.reply_text(msg)

    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")
        import traceback
        traceback.print_exc()

async def cmd_check_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text(
                "❌ Usage: /check <signal_id>\n\nExemple: /check 1\n\nUtilisez /debug pour voir les IDs"
            )
            return

        signal_id = int(context.args[0])

        with engine.connect() as conn:    
            signal = conn.execute(    
                text("SELECT id, pair, direction, ts_enter, result FROM signals WHERE id = :sid"),    
                {"sid": signal_id}    
            ).fetchone()    
            
        if not signal:    
            await update.message.reply_text(f"❌ Signal #{signal_id} introuvable")    
            return    
            
        sid, pair, direction, ts_enter, result = signal    
            
        if result:    
            await update.message.reply_text(f"ℹ️ Signal #{sid} déjà vérifié: {result}")    
            return    
            
        msg = await update.message.reply_text(f"🔍 Vérification du signal #{sid}...")    
            
        try:    
            verification_result = await verify_signal_manual(signal_id, context.application)    
                
            if verification_result:    
                with engine.connect() as conn:    
                    updated = conn.execute(    
                        text("SELECT result, gale_level FROM signals WHERE id = :sid"),    
                        {"sid": signal_id}    
                    ).fetchone()    
                    
                if updated and updated[0]:    
                    result_emoji = "✅" if updated[0] == "WIN" else "❌"    
                    gale_names = ["Signal initial", "Gale 1", "Gale 2"]    
                    gale_text = gale_names[updated[1]] if updated[1] < 3 else f"Gale {updated[1]}"    
                        
                    await msg.edit_text(    
                        f"{result_emoji} Signal #{sid} vérifié!\n\n"    
                        f"{pair} {direction}\n"    
                        f"Résultat: {updated[0]}\n"    
                        f"Niveau: {gale_text}"    
                    )    
                else:    
                    await msg.edit_text(f"✅ Signal #{sid} vérifié!")    
            else:    
                await msg.edit_text(f"❌ Impossible de vérifier le signal #{sid}")    
        except Exception as e:    
            await msg.edit_text(f"❌ Erreur:\n{str(e)[:100]}")

    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")

# NOUVELLE COMMANDE: Debug détaillé de la vérification
async def cmd_debug_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande de debug détaillée pour la vérification"""
    try:
        now_utc = get_utc_now()
        now_haiti = get_haiti_now()
        
        with engine.connect() as conn:
            # Signaux sans résultat
            pending = conn.execute(
                text("""
                    SELECT id, pair, direction, ts_enter, confidence 
                    FROM signals 
                    WHERE result IS NULL 
                    ORDER BY ts_enter DESC
                """)
            ).fetchall()
            
            # Derniers signaux vérifiés
            verified = conn.execute(
                text("""
                    SELECT id, pair, direction, result, gale_level, ts_enter 
                    FROM signals 
                    WHERE result IS NOT NULL 
                    ORDER BY id DESC LIMIT 5
                """)
            ).fetchall()
        
        msg = f"🔍 **DEBUG VÉRIFICATION**\n\n"
        msg += f"⏰ UTC: {now_utc.strftime('%H:%M:%S')}\n"
        msg += f"⏰ Haïti: {now_haiti.strftime('%H:%M:%S')}\n"
        msg += f"📊 Signaux en attente: {len(pending)}\n\n"
        
        if pending:
            msg += "⏳ **EN ATTENTE:**\n"
            for sig in pending[:5]:  # Limiter à 5
                sid, pair, direction, ts_enter, conf = sig
                
                # Analyser le timing
                try:
                    entry_time = datetime.fromisoformat(ts_enter.replace('Z', '+00:00'))
                    if entry_time.tzinfo is None:
                        entry_time = entry_time.replace(tzinfo=timezone.utc)
                    
                    end_time = entry_time + timedelta(minutes=15)
                    time_left = (end_time - now_utc).total_seconds() / 60
                    
                    status = "✅ PRÊT" if time_left <= 0 else f"⏳ {time_left:.1f} min"
                    
                    msg += f"#{sid} {pair} {direction} - {status}\n"
                except Exception as e:
                    msg += f"#{sid} {pair} - ❌ Erreur: {e}\n"
        
        if verified:
            msg += "\n✅ **DERNIERS VÉRIFIÉS:**\n"
            for sig in verified:
                sid, pair, direction, result, gale, ts_enter = sig
                emoji = "✅" if result == "WIN" else "❌"
                gale_text = f" (Gale {gale})" if gale else ""
                msg += f"#{sid} {pair} {direction} {emoji} {result}{gale_text}\n"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur debug: {e}")

# === ENVOI DE SIGNAUX ===

async def send_pre_signal(pair, entry_time_haiti, app):
    now_haiti = get_haiti_now()
    print(f"\n📤 Tentative signal {pair} - {now_haiti.strftime('%H:%M:%S')} (Haïti)")

    try:
        params = BEST_PARAMS.get(pair, {})
        df = get_cached_ohlc(pair, TIMEFRAME_M1, outputsize=400)

        if df is None or len(df) < 50:    
            print("❌ Pas assez de données")    
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

async def verify_signal_manual(signal_id, app):
    try:
        with engine.connect() as conn:
            signal = conn.execute(
                text("SELECT pair, direction, ts_enter FROM signals WHERE id = :sid"),
                {"sid": signal_id}
            ).fetchone()

        if not signal:
            return False

        pair, direction, ts_enter_str = signal    
            
        try:    
            ts_enter = datetime.fromisoformat(ts_enter_str.replace('Z', '+00:00'))    
        except:    
            ts_enter = datetime.fromisoformat(ts_enter_str)    
            if ts_enter.tzinfo is None:    
                ts_enter = ts_enter.replace(tzinfo=timezone.utc)    
            
        df = get_cached_ohlc(pair, TIMEFRAME_M1, outputsize=100)    
            
        if df is None or len(df) == 0:    
            return False    
            
        df_filtered = df[df.index >= ts_enter]    
            
        if len(df_filtered) == 0:    
            return False    
            
        entry_price = df_filtered.iloc[0]['close']    
        max_candles = min(3, len(df_filtered))    
            
        for i in range(max_candles):    
            if i >= len(df_filtered):    
                break    
                    
            candle = df_filtered.iloc[i]    
            open_price = entry_price if i == 0 else df_filtered.iloc[i]['open']    
            close_price = candle['close']    
                
            win = (close_price > open_price) if direction == 'CALL' else (close_price < open_price)    
                
            if win:    
                with engine.begin() as conn:    
                    conn.execute(    
                        text("UPDATE signals SET result='WIN', gale_level=:gale WHERE id=:sid"),    
                        {"gale": i, "sid": signal_id}    
                    )    
                print(f"✅ WIN au niveau {i}")    
                return True    
            
        with engine.begin() as conn:    
            conn.execute(    
                text("UPDATE signals SET result='LOSE', gale_level=2 WHERE id=:sid"),    
                {"sid": signal_id}    
            )    
        print(f"❌ LOSE")    
        return True

    except Exception as e:
        print(f"❌ Erreur verify_signal_manual: {e}")
        return False

# === FILE DE SIGNAUX ===

async def process_signal_queue(app):
    global signal_queue_running

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
                try:    
                    await verify_signal_manual(signal_id, app)    
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
    now_haiti = get_haiti_now()

    if now_haiti.weekday() > 4:
        print("🏖️ Weekend")
        return

    print(f"\n📅 Démarrage - {now_haiti.strftime('%H:%M:%S')}")
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
    print(f"⏰ Début quotidien: {START_HOUR_HAITI}h00 AM (Haïti)")
    print(f"📊 Signaux: Séquentiels après vérification")
    print("="*60 + "\n")

    ensure_db()
    auto_verifier = AutoResultVerifier(engine, TWELVEDATA_API_KEY)

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('stats', cmd_stats))
    app.add_handler(CommandHandler('verify', cmd_verify))
    app.add_handler(CommandHandler('test', cmd_test))
    app.add_handler(CommandHandler('force', cmd_force))
    app.add_handler(CommandHandler('debug', cmd_debug))
    app.add_handler(CommandHandler('check', cmd_check_signal))
    app.add_handler(CommandHandler('debug_verify', cmd_debug_verification))  # NOUVELLE COMMANDE

    sched.start()

    # CORRECTION: Planifier la vérification automatique toutes les 5 minutes
    sched.add_job(
        auto_verifier.verify_pending_signals,
        'interval',
        minutes=5,
        id='auto_verify'
    )

    if (now_haiti.hour >= START_HOUR_HAITI and now_haiti.hour < 18 and
    now_haiti.weekday() <= 4 and not signal_queue_running):
        print("🚀 Démarrage immédiat")
        asyncio.create_task(process_signal_queue(app))

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
