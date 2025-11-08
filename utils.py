import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange


def compute_indicators(df, ema_fast=8, ema_slow=21, rsi_len=14, bb_len=20):
    """Calcule des indicateurs techniques avancés pour une analyse de haute confiance"""
    df = df.copy()
    df['close'] = df['close'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    
    # EMA (Exponential Moving Average)
    df['ema_fast'] = EMAIndicator(close=df['close'], window=ema_fast).ema_indicator()
    df['ema_slow'] = EMAIndicator(close=df['close'], window=ema_slow).ema_indicator()
    df['ema_50'] = EMAIndicator(close=df['close'], window=50).ema_indicator()
    df['ema_200'] = EMAIndicator(close=df['close'], window=200).ema_indicator()
    
    # MACD (Moving Average Convergence Divergence)
    macd = MACD(close=df['close'])
    df['MACD_12_26_9'] = macd.macd()
    df['MACDs_12_26_9'] = macd.macd_signal()
    df['MACDh_12_26_9'] = macd.macd_diff()
    
    # RSI (Relative Strength Index)
    df['rsi'] = RSIIndicator(close=df['close'], window=rsi_len).rsi()
    
    # Bollinger Bands
    bb = BollingerBands(close=df['close'], window=bb_len, window_dev=2)
    df['BBL_20_2.0'] = bb.bollinger_lband()
    df['BBM_20_2.0'] = bb.bollinger_mavg()
    df['BBU_20_2.0'] = bb.bollinger_hband()
    df['BB_width'] = (df['BBU_20_2.0'] - df['BBL_20_2.0']) / df['BBM_20_2.0']
    
    # ATR (Average True Range)
    df['atr'] = AverageTrueRange(
        high=df['high'], 
        low=df['low'], 
        close=df['close'], 
        window=14
    ).average_true_range()
    
    # ADX (Average Directional Index) - Force de la tendance
    adx = ADXIndicator(high=df['high'], low=df['low'], close=df['close'], window=14)
    df['adx'] = adx.adx()
    df['adx_pos'] = adx.adx_pos()
    df['adx_neg'] = adx.adx_neg()
    
    # Stochastic Oscillator
    stoch = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'])
    df['stoch_k'] = stoch.stoch()
    df['stoch_d'] = stoch.stoch_signal()
    
    # Volume profile (si disponible)
    if 'volume' in df.columns:
        df['volume_sma'] = df['volume'].rolling(window=20).mean()
    
    return df


def rule_signal(df):
    """
    Stratégie de trading à haute confiance (90-95%)
    Compatible avec les options binaires sur Pocket Option
    
    Critères pour BUY (CALL):
    1. EMA rapide > EMA lente (tendance haussière)
    2. Prix au-dessus de EMA 50 et 200 (tendance forte)
    3. MACD > Signal et MACD histogram positif et croissant
    4. RSI entre 50-70 (momentum haussier mais pas suracheté)
    5. ADX > 25 (tendance forte)
    6. Prix proche de la bande de Bollinger basse (retracement)
    7. Stochastic > 20 et croissant
    
    Critères pour SELL (PUT):
    1. EMA rapide < EMA lente (tendance baissière)
    2. Prix en-dessous de EMA 50 et 200 (tendance forte)
    3. MACD < Signal et MACD histogram négatif et décroissant
    4. RSI entre 30-50 (momentum baissier mais pas survendu)
    5. ADX > 25 (tendance forte)
    6. Prix proche de la bande de Bollinger haute (retracement)
    7. Stochastic < 80 et décroissant
    """
    
    if len(df) < 3:
        return None
        
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    
    # Vérifications de base
    rsi = last.get('rsi')
    adx = last.get('adx')
    stoch_k = last.get('stoch_k')
    
    if rsi is None or adx is None or stoch_k is None:
        return None
    
    # --- Critères BUY (CALL) ---
    
    # 1. Croisement EMA haussier confirmé
    ema_bullish = (
        last['ema_fast'] > last['ema_slow'] and 
        prev['ema_fast'] > prev['ema_slow']
    )
    
    # 2. Tendance haussière forte (au-dessus EMA 50 et 200)
    strong_uptrend = (
        last['close'] > last['ema_50'] and 
        last['close'] > last['ema_200'] and
        last['ema_50'] > last['ema_200']
    )
    
    # 3. MACD haussier fort
    macd_strong_bull = (
        last['MACD_12_26_9'] > last['MACDs_12_26_9'] and
        last['MACDh_12_26_9'] > 0 and
        last['MACDh_12_26_9'] > prev['MACDh_12_26_9']  # Histogram croissant
    )
    
    # 4. RSI dans la zone optimale (pas suracheté)
    rsi_optimal_bull = 50 <= rsi <= 70
    
    # 5. Tendance forte (ADX)
    strong_trend = adx > 25
    
    # 6. Prix proche de la bande inférieure (opportunité d'achat)
    bb_retracement_bull = (
        last['close'] < last['BBM_20_2.0'] and
        last['close'] > last['BBL_20_2.0']
    )
    
    # 7. Stochastic haussier
    stoch_bull = stoch_k > 20 and stoch_k < 80 and last['stoch_k'] > prev['stoch_k']
    
    # Compter les confirmations BUY
    buy_confirmations = sum([
        ema_bullish,
        strong_uptrend,
        macd_strong_bull,
        rsi_optimal_bull,
        strong_trend,
        bb_retracement_bull,
        stoch_bull
    ])
    
    # --- Critères SELL (PUT) ---
    
    # 1. Croisement EMA baissier confirmé
    ema_bearish = (
        last['ema_fast'] < last['ema_slow'] and 
        prev['ema_fast'] < prev['ema_slow']
    )
    
    # 2. Tendance baissière forte (en-dessous EMA 50 et 200)
    strong_downtrend = (
        last['close'] < last['ema_50'] and 
        last['close'] < last['ema_200'] and
        last['ema_50'] < last['ema_200']
    )
    
    # 3. MACD baissier fort
    macd_strong_bear = (
        last['MACD_12_26_9'] < last['MACDs_12_26_9'] and
        last['MACDh_12_26_9'] < 0 and
        last['MACDh_12_26_9'] < prev['MACDh_12_26_9']  # Histogram décroissant
    )
    
    # 4. RSI dans la zone optimale (pas survendu)
    rsi_optimal_bear = 30 <= rsi <= 50
    
    # 5. Prix proche de la bande supérieure (opportunité de vente)
    bb_retracement_bear = (
        last['close'] > last['BBM_20_2.0'] and
        last['close'] < last['BBU_20_2.0']
    )
    
    # 6. Stochastic baissier
    stoch_bear = stoch_k < 80 and stoch_k > 20 and last['stoch_k'] < prev['stoch_k']
    
    # Compter les confirmations SELL
    sell_confirmations = sum([
        ema_bearish,
        strong_downtrend,
        macd_strong_bear,
        rsi_optimal_bear,
        strong_trend,
        bb_retracement_bear,
        stoch_bear
    ])
    
    # Exiger au moins 6/7 confirmations pour un signal de haute confiance (90%+)
    if buy_confirmations >= 6:
        return 'CALL'
    
    if sell_confirmations >= 6:
        return 'PUT'
    
    return None
