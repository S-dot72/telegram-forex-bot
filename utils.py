import pandas as pd
import pandas_ta as ta

def compute_indicators(df, ema_fast=8, ema_slow=21, rsi_len=14, bb_len=20):
    df = df.copy()
    df['close'] = df['close'].astype(float)
    df['ema_fast'] = ta.ema(df['close'], length=ema_fast)
    df['ema_slow'] = ta.ema(df['close'], length=ema_slow)
    macd = ta.macd(df['close'])
    df = pd.concat([df, macd], axis=1)
    df['rsi'] = ta.rsi(df['close'], length=rsi_len)
    bb = ta.bbands(df['close'], length=bb_len)
    df = pd.concat([df, bb], axis=1)
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    return df


def rule_signal(df):
    last = df.iloc[-1]
    prev = df.iloc[-2]
    bullish = (prev['ema_fast'] <= prev['ema_slow']) and (last['ema_fast'] > last['ema_slow'])
    bearish = (prev['ema_fast'] >= prev['ema_slow']) and (last['ema_fast'] < last['ema_slow'])
    macd_bull = (last['MACD_12_26_9'] > last['MACDs_12_26_9']) and (last['MACDh_12_26_9'] > 0)
    macd_bear = (last['MACD_12_26_9'] < last['MACDs_12_26_9']) and (last['MACDh_12_26_9'] < 0)
    rsi = last.get('rsi', None)
    if bullish and macd_bull and (rsi is not None and 45 <= rsi <= 65):
        return 'CALL'
    if bearish and macd_bear and (rsi is not None and 35 <= rsi <= 55):
        return 'PUT'
    return None
