"""Grid-search + walk-forward backtester to find best indicator params per pair.
Outputs best_params.json and a CSV with detailed results.
"""
import itertools, json
import pandas as pd
import numpy as np
import requests
from datetime import timedelta
from utils import compute_indicators, rule_signal
from config import TWELVEDATA_API_KEY, PAIRS, TIMEFRAME_M1, WALK_TOTAL_DAYS, WALK_DAYS_WINDOW, WALK_DAYS_TEST

TD = 'https://api.twelvedata.com/time_series'

def fetch(pair, interval='1min', outputsize=20000):
    symbol = pair.replace('/','')
    params = {'symbol': symbol, 'interval': interval, 'outputsize': outputsize, 'apikey': TWELVEDATA_API_KEY, 'format':'JSON'}
    r = requests.get(TD, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if 'values' not in j:
        raise RuntimeError('TD error: '+str(j))
    df = pd.DataFrame(j['values'])[::-1].reset_index(drop=True)
    df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].astype(float)
    df.index = pd.to_datetime(df['datetime'])
    return df

# parameter grid (kept small to limit API/time)
EMA_FAST = [5,8,10]
EMA_SLOW = [20,21,30]
RSI_LEN = [10,14,21]
BB_LEN = [14,20]


def evaluate_params(df, ema_f, ema_s, rsi_l, bb_l):
    df2 = compute_indicators(df, ema_fast=ema_f, ema_slow=ema_s, rsi_len=rsi_l, bb_len=bb_l)
    wins = 0
    total = 0
    # simulate simple walk-forward per minute over available df
    for i in range(50, len(df2)-1):
        window = df2.iloc[:i+1]
        sig = rule_signal(window)
        if sig:
            entry = df2['close'].iloc[i]
            exit_price = df2['close'].iloc[i+1]
            win = (exit_price>entry) if sig=='CALL' else (exit_price<entry)
            total += 1
            wins += 1 if win else 0
    return {'wins':wins,'total':total,'winrate':(wins/total if total else 0)}


def grid_search_for_pair(pair):
    print('Fetching',pair)
    df = fetch(pair, outputsize=20000)
    best = None
    results = []
    for (ef, es, rl, bl) in itertools.product(EMA_FAST, EMA_SLOW, RSI_LEN, BB_LEN):
        if ef>=es: continue
        res = evaluate_params(df, ef, es, rl, bl)
        res_record = {'pair':pair,'ema_fast':ef,'ema_slow':es,'rsi':rl,'bb':bl,'wins':res['wins'],'total':res['total'],'winrate':res['winrate']}
        results.append(res_record)
        if res['total']>50 and (best is None or res['winrate']>best['winrate']):
            best = res_record
    # save detailed csv
    pd.DataFrame(results).to_csv(f'results_{pair.replace("/","")}.csv', index=False)
    return best

if __name__=='__main__':
    all_best = {}
    for p in PAIRS:
        try:
            best = grid_search_for_pair(p)
            all_best[p] = best
            print('Best for',p,':',best)
        except Exception as e:
            print('Error for',p,e)
    with open('best_params.json','w') as f:
        json.dump(all_best, f, indent=2)
    print('Saved best_params.json')
