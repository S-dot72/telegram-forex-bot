# Simple walk-forward backtester (reference)
import pandas as pd
import requests
from datetime import timedelta
from utils import compute_indicators, rule_signal
from config import TWELVEDATA_API_KEY, PAIRS, TIMEFRAME_M5

TD = 'https://api.twelvedata.com/time_series'

def fetch(pair, interval='1min', outputsize=10000):
    symbol = pair.replace('/','')
    params = {'symbol': symbol, 'interval': interval, 'outputsize': outputsize, 'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TD, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    if 'values' not in j:
        raise RuntimeError('TD error: '+str(j))
    df = pd.DataFrame(j['values'])[::-1].reset_index(drop=True)
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    df.index = pd.to_datetime(df['datetime'])
    return df

if __name__=='__main__':
    for p in PAIRS:
        print('Backtesting',p)
        df = fetch(p, outputsize=20000)
        df = compute_indicators(df)
        wins = 0
        total = 0
        for i in range(30, len(df)-1):
            window = df.iloc[:i+1]
            sig = rule_signal(window)
            if sig:
                entry = df['close'].iloc[i]
                exit_price = df['close'].iloc[i+1]
                win = (exit_price>entry) if sig=='CALL' else (exit_price<entry)
                total += 1
                wins += 1 if win else 0
        print('Results:', total, 'trades, wins', wins, 'winrate', (wins/total if total else 0))
