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
    🎯 STRATÉGIE ULTRA STRICTE POUR 90%+ WIN RATE
    
    Timeframe: M1 (1 minute)
    Mode: SANS GALE - Chaque signal doit être gagnant
    Objectif: 10 signaux/jour avec 90%+ de réussite
    
    CRITÈRES EXTRÊMEMENT STRICTS:
    - Tendance forte confirmée (ADX > 30)
    - Tous les indicateurs alignés (pas 2/3, mais 100%)
    - Zones de prix optimales (pas d'extrêmes)
    - Momentum fort et croissant
    - Volatilité contrôlée
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
    # CRITÈRE 1: TENDANCE FORTE OBLIGATOIRE
    # ========================================
    # ADX > 30 = tendance très forte (essentiel pour M1)
    if adx < 30:
        return None
    
    # ========================================
    # CRITÈRE 2: VOLATILITÉ CONTRÔLÉE
    # ========================================
    # ATR ne doit pas être trop élevé (éviter les marchés chaotiques)
    atr = last.get('atr', 0)
    atr_sma = df['atr'].rolling(20).mean().iloc[-1]
    if atr > atr_sma * 1.5:  # Volatilité anormalement haute
        return None
    
    # ========================================
    # CRITÈRE 3: RSI DANS ZONE OPTIMALE
    # ========================================
    # Pour CALL: RSI entre 45-65 (ni survendu ni surachat)
    # Pour PUT: RSI entre 35-55
    rsi_too_low = rsi < 35
    rsi_too_high = rsi > 65
    
    if rsi_too_low or rsi_too_high:
        return None
    
    # ========================================
    # ANALYSE CALL (BUY)
    # ========================================
    
    # Direction EMA - DOIT être haussière
    ema_bullish = (
        last['ema_fast'] > last['ema_slow'] and
        last['ema_slow'] > last['ema_50'] and
        last['close'] > last['ema_50'] and
        last['close'] > last['ema_200']
    )
    
    # MACD - DOIT être haussier ET croissant
    macd_bullish = (
        macd > macd_signal and
        macd_hist > 0 and
        macd_hist > prev['MACDh_12_26_9'] and  # Momentum croissant
        prev['MACDh_12_26_9'] > prev2['MACDh_12_26_9']  # Momentum confirmé
    )
    
    # RSI - DOIT être dans la zone haussière
    rsi_bullish = 45 < rsi < 65
    
    # Stochastic - DOIT confirmer sans être en surachat
    stoch_bullish = (
        stoch_k > stoch_d and
        20 < stoch_k < 80 and
        stoch_k > prev['stoch_k']  # Momentum haussier
    )
    
    # ADX - Tendance haussière dominante
    adx_bullish = last['adx_pos'] > last['adx_neg']
    
    # Prix au-dessus des bandes de Bollinger moyennes
    bb_position = (last['close'] - last['BBL_20_2.0']) / (last['BBU_20_2.0'] - last['BBL_20_2.0'])
    bb_bullish = 0.3 < bb_position < 0.7  # Pas d'extrêmes
    
    # Momentum de prix positif
    price_momentum_up = (
        last['close'] > prev['close'] and
        prev['close'] > prev2['close']
    )
    
    # ========================================
    # DÉCISION CALL: TOUS LES CRITÈRES REQUIS
    # ========================================
    call_conditions = [
        ema_bullish,
        macd_bullish,
        rsi_bullish,
        stoch_bullish,
        adx_bullish,
        bb_bullish,
        price_momentum_up
    ]
    
    if all(call_conditions):
        return 'CALL'
    
    # ========================================
    # ANALYSE PUT (SELL)
    # ========================================
    
    # Direction EMA - DOIT être baissière
    ema_bearish = (
        last['ema_fast'] < last['ema_slow'] and
        last['ema_slow'] < last['ema_50'] and
        last['close'] < last['ema_50'] and
        last['close'] < last['ema_200']
    )
    
    # MACD - DOIT être baissier ET décroissant
    macd_bearish = (
        macd < macd_signal and
        macd_hist < 0 and
        macd_hist < prev['MACDh_12_26_9'] and  # Momentum décroissant
        prev['MACDh_12_26_9'] < prev2['MACDh_12_26_9']  # Momentum confirmé
    )
    
    # RSI - DOIT être dans la zone baissière
    rsi_bearish = 35 < rsi < 55
    
    # Stochastic - DOIT confirmer sans être en survente
    stoch_bearish = (
        stoch_k < stoch_d and
        20 < stoch_k < 80 and
        stoch_k < prev['stoch_k']  # Momentum baissier
    )
    
    # ADX - Tendance baissière dominante
    adx_bearish = last['adx_neg'] > last['adx_pos']
    
    # Prix en-dessous des bandes de Bollinger moyennes
    bb_bearish = 0.3 < bb_position < 0.7  # Pas d'extrêmes
    
    # Momentum de prix négatif
    price_momentum_down = (
        last['close'] < prev['close'] and
        prev['close'] < prev2['close']
    )
    
    # ========================================
    # DÉCISION PUT: TOUS LES CRITÈRES REQUIS
    # ========================================
    put_conditions = [
        ema_bearish,
        macd_bearish,
        rsi_bearish,
        stoch_bearish,
        adx_bearish,
        bb_bearish,
        price_momentum_down
    ]
    
    if all(put_conditions):
        return 'PUT'
    
    # Si aucun signal n'est assez fort, NE PAS TRADER
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
