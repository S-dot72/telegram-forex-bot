# Telegram Forex Signal Bot — Optimized (with Backtester)

This project includes:
- signal_bot.py : production bot that schedules and sends signals (M1 indicators)
- backtester_optimized.py : grid-search + walk-forward backtester to find best parameters per pair
- backtester.py : simple walk-forward backtester (reference)
- DB schema, utils, config, and Docker/Procfile for deployment

## Quick start
1. Copy `.env.example` to `.env` and fill your keys
2. pip install -r requirements.txt
3. Run backtester to find best params:
   ```bash
   python backtester_optimized.py
   ```
4. Update `.env` with chosen params or let the bot load them from `best_params.json` created by the backtester.
5. Run bot locally: `python signal_bot.py` or deploy to Railway/Docker.
