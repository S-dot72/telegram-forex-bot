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


def rule_signal_ultra_strict(df):
    """
    🎯 STRATÉGIE ÉQUILIBRÉE POUR 90%+ WIN RATE
    
    Timeframe: M1 (1 minute)
    Mode: SANS GALE - Chaque signal doit être gagnant
    Objectif: 8-10 signaux/jour avec 90%+ de réussite
    
    CRITÈRES STRICTS MAIS RÉALISTES:
    - Tendance confirmée (ADX > 25)
    - 5/6 indicateurs alignés (83% minimum)
    - Zones de prix optimales
    - Momentum présent
    - Volatilité acceptable
    """
    
    if len(df) < 10:
        return None
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    
    # Vérifications de base
    rsi = last.get('rsi')
    adx = last.get('adx')
    stoch_k = last.get('stoch_k')
    stoch_d = last.get('stoch_d')
    macd = last.get('MACD_12_26_9')
    macd_signal = last.get('MACDs_12_26_9')
    macd_hist = last.get('MACDh_12_26_9')
    
    if None in [rsi, adx, stoch_k, stoch_d, macd, macd_signal, macd_hist]:
        return None
    
    # ========================================
    # CRITÈRE 1: TENDANCE CONFIRMÉE
    # ========================================
    # ADX > 25 = tendance forte (réaliste pour M1)
    if adx < 25:
        return None
    
    # ========================================
    # CRITÈRE 2: VOLATILITÉ ACCEPTABLE
    # ========================================
    # ATR ne doit pas être extrême
    atr = last.get('atr', 0)
    atr_sma = df['atr'].rolling(20).mean().iloc[-1]
    if atr > atr_sma * 1.8:  # Volatilité trop haute
        return None
    
    # ========================================
    # CRITÈRE 3: RSI DANS ZONE SÉCURISÉE
    # ========================================
    # Pas d'extrêmes (ni survente ni surachat extrême)
    if rsi < 30 or rsi > 70:
        return None
    
    # ========================================
    # ANALYSE CALL (BUY) - 5/6 CRITÈRES
    # ========================================
    
    call_signals = []
    
    # 1. Direction EMA principale
    ema_bullish_main = last['ema_fast'] > last['ema_slow']
    call_signals.append(ema_bullish_main)
    
    # 2. Prix au-dessus des EMAs clés
    price_above_emas = (
        last['close'] > last['ema_fast'] and
        last['close'] > last['ema_50']
    )
    call_signals.append(price_above_emas)
    
    # 3. MACD haussier avec momentum
    macd_bullish = (
        macd > macd_signal and
        macd_hist > 0 and
        macd_hist > prev['MACDh_12_26_9']
    )
    call_signals.append(macd_bullish)
    
    # 4. RSI dans zone haussière
    rsi_bullish = 45 < rsi < 70
    call_signals.append(rsi_bullish)
    
    # 5. Stochastic confirme
    stoch_bullish = (
        stoch_k > stoch_d and
        20 < stoch_k < 85
    )
    call_signals.append(stoch_bullish)
    
    # 6. ADX tendance haussière
    adx_bullish = last['adx_pos'] > last['adx_neg']
    call_signals.append(adx_bullish)
    
    # DÉCISION CALL: 5/6 critères minimum (83%)
    call_score = sum(call_signals)
    if call_score >= 5:
        return 'CALL'
    
    # ========================================
    # ANALYSE PUT (SELL) - 5/6 CRITÈRES
    # ========================================
    
    put_signals = []
    
    # 1. Direction EMA principale
    ema_bearish_main = last['ema_fast'] < last['ema_slow']
    put_signals.append(ema_bearish_main)
    
    # 2. Prix en-dessous des EMAs clés
    price_below_emas = (
        last['close'] < last['ema_fast'] and
        last['close'] < last['ema_50']
    )
    put_signals.append(price_below_emas)
    
    # 3. MACD baissier avec momentum
    macd_bearish = (
        macd < macd_signal and
        macd_hist < 0 and
        macd_hist < prev['MACDh_12_26_9']
    )
    put_signals.append(macd_bearish)
    
    # 4. RSI dans zone baissière
    rsi_bearish = 30 < rsi < 55
    put_signals.append(rsi_bearish)
    
    # 5. Stochastic confirme
    stoch_bearish = (
        stoch_k < stoch_d and
        15 < stoch_k < 80
    )
    put_signals.append(stoch_bearish)
    
    # 6. ADX tendance baissière
    adx_bearish = last['adx_neg'] > last['adx_pos']
    put_signals.append(adx_bearish)
    
    # DÉCISION PUT: 5/6 critères minimum (83%)
    put_score = sum(put_signals)
    if put_score >= 5:
        return 'PUT'
    
    # Si moins de 5/6 critères, NE PAS TRADER
    return None


def rule_signal(df):
    """
    Stratégie standard (utilisée pour compatibilité)
    Pour les nouveaux signaux, utiliser rule_signal_ultra_strict()
    """
    
    if len(df) < 3:
        return None
        
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Vérifications de base
    rsi = last.get('rsi')
    adx = last.get('adx')
    stoch_k = last.get('stoch_k')
    
    if rsi is None or adx is None or stoch_k is None:
        return None
    
    # === CRITÈRES PRINCIPAUX (2/3 requis) ===
    
    # 1. Direction EMA
    ema_bullish = last['ema_fast'] > last['ema_slow']
    ema_bearish = last['ema_fast'] < last['ema_slow']
    
    # 2. MACD confirme
    macd_bullish = last['MACD_12_26_9'] > last['MACDs_12_26_9']
    macd_bearish = last['MACD_12_26_9'] < last['MACDs_12_26_9']
    
    # 3. RSI dans zone tradable (pas d'extrêmes)
    rsi_tradable = 25 < rsi < 75
    rsi_bullish = rsi > 40
    rsi_bearish = rsi < 60
    
    # === CRITÈRES SECONDAIRES (1/4 requis pour confirmation) ===
    
    # Momentum MACD
    macd_momentum_up = last['MACDh_12_26_9'] > 0
    macd_momentum_down = last['MACDh_12_26_9'] < 0
    
    # Tendance confirmée par EMA 50
    above_ema50 = last['close'] > last['ema_50']
    below_ema50 = last['close'] < last['ema_50']
    
    # Tendance présente (ADX)
    has_trend = adx > 15
    
    # Stochastic favorable
    stoch_bullish = 20 < stoch_k < 85
    stoch_bearish = 15 < stoch_k < 80
    
    # === LOGIQUE BUY (CALL) ===
    
    # Compter critères principaux BUY
    buy_main = [
        ema_bullish,
        macd_bullish,
        rsi_tradable and rsi_bullish
    ]
    buy_main_count = sum(buy_main)
    
    # Compter critères secondaires BUY
    buy_secondary = [
        macd_momentum_up,
        above_ema50,
        has_trend,
        stoch_bullish
    ]
    buy_secondary_count = sum(buy_secondary)
    
    # === LOGIQUE SELL (PUT) ===
    
    # Compter critères principaux SELL
    sell_main = [
        ema_bearish,
        macd_bearish,
        rsi_tradable and rsi_bearish
    ]
    sell_main_count = sum(sell_main)
    
    # Compter critères secondaires SELL
    sell_secondary = [
        macd_momentum_down,
        below_ema50,
        has_trend,
        stoch_bearish
    ]
    sell_secondary_count = sum(sell_secondary)
    
    # DÉCISION: 2/3 principaux + 1/4 secondaires minimum
    if buy_main_count >= 2 and buy_secondary_count >= 1:
        return 'CALL'
    
    if sell_main_count >= 2 and sell_secondary_count >= 1:
        return 'PUT'
    
    return None
