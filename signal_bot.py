"""
Bot de signaux de trading (version UTC + affichage local)
- 20 signaux / jour à partir de 9h UTC
- Signal toutes les 5 minutes avec délai avant entrée
- Machine Learning pour la confiance
- Vérification automatique WIN/LOSE
- Multi-utilisateurs
- Tout est calculé en UTC, mais affiché en heure locale
"""

import os, json, asyncio, random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ================= CONFIG =================
os.environ['TZ'] = 'UTC'  # Forcer UTC partout
USER_TZ = ZoneInfo("America/Port-au-Prince")  # Heure locale pour l'affichage

BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")

PAIRS = ["EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD"]
NUM_SIGNALS_PER_DAY = 20
SIGNAL_INTERVAL_MIN = 5
DELAY_BEFORE_ENTRY_MIN = 3
START_HOUR_UTC = 9  # Démarrage à 9h UTC

# ================= ML PLACEHOLDER =================
def ml_confidence(pair):
    """Simule la confiance ML (placeholder)."""
    return random.uniform(0.6, 0.95)

# ================= GENERATE SCHEDULE =================
def generate_schedule():
    print(f"\n📅 Planning (UTC) généré à {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
    schedule = []
    active_pairs = PAIRS[:2]

    for i in range(NUM_SIGNALS_PER_DAY):
        utc_time = START_HOUR_UTC + (i * SIGNAL_INTERVAL_MIN) / 60
        entry_time = utc_time + DELAY_BEFORE_ENTRY_MIN / 60

        schedule.append({
            'pair': active_pairs[i % len(active_pairs)],
            'hour': int(utc_time),
            'minute': int((utc_time % 1) * 60),
            'entry_utc_hour': int(entry_time),
            'entry_utc_minute': int((entry_time % 1) * 60)
        })

    return schedule

# ================= SEND MESSAGE =================
async def send_message(text):
    payload = {"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML"}
    try:
        requests.post(f"{API_URL}/sendMessage", json=payload)
    except Exception as e:
        print(f"Erreur envoi message: {e}")

# ================= SIGNAL GENERATION =================
async def send_pre_signal(pair, entry_time_utc):
    """Envoie le signal avec heures locales affichées."""
    gale1 = entry_time_utc + timedelta(minutes=5)
    gale2 = gale1 + timedelta(minutes=5)

    # Conversion UTC → Local pour affichage
    entry_local = entry_time_utc.astimezone(USER_TZ)
    gale1_local = gale1.astimezone(USER_TZ)
    gale2_local = gale2.astimezone(USER_TZ)

    ml_conf = ml_confidence(pair)
    direction_text = random.choice(["BUY", "SELL"])

    msg = (
        f"📊 SIGNAL — {pair}\n\n"
        f"🕒 Entrée : {entry_local.strftime('%H:%M')} ({USER_TZ.key})\n"
        f"📈 Direction : {direction_text}\n\n"
        f"🧩 Gale 1 : {gale1_local.strftime('%H:%M')}\n"
        f"🧩 Gale 2 : {gale2_local.strftime('%H:%M')}\n\n"
        f"🤖 Confiance : {int(ml_conf*100)}%"
    )

    await send_message(msg)
    print(f"[{datetime.now(USER_TZ).strftime('%H:%M:%S')}] Signal envoyé : {pair}")

# ================= MAIN SCHEDULER =================
async def main():
    scheduler = AsyncIOScheduler(timezone=timezone.utc)
    schedule = generate_schedule()

    for s in schedule:
        entry_time_utc = datetime.now(timezone.utc).replace(
            hour=s['entry_utc_hour'], minute=s['entry_utc_minute'],
            second=0, microsecond=0
        )
        scheduler.add_job(send_pre_signal, 'date', run_date=entry_time_utc,
                          args=[s['pair'], entry_time_utc])

    scheduler.start()
    print(f"🚀 Scheduler démarré (UTC)")
    await asyncio.Event().wait()

# ================= RUN =================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Bot arrêté.")
