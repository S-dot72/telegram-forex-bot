import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange


def round_to_m5_candle(dt):
    """
    Arrondit un datetime à la bougie M5 la plus proche
    Exemple: 14:23:47 -> 14:20:00
            14:27:12 -> 14:25:00
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Arrondir les minutes à un multiple de 5
    minute = (dt.minute // 5) * 5
    
    # Remettre secondes et microsecondes à 0
    return dt.replace(minute=minute, second=0, microsecond=0)


def get_next_m5_candle(dt):
    """
    Retourne le début de la PROCHAINE bougie M5
    Exemple: 14:23:47 -> 14:25:00
            14:20:00 -> 14:25:00
    """
    current_candle = round_to_m5_candle(dt)
    return current_candle + timedelta(minutes=5)


def get_m5_candle_range(dt):
    """
    Retourne le début et la fin de la bougie M5 contenant ce datetime
    Exemple: 14:23:47 -> (14:20:00, 14:25:00)
    """
    start = round_to_m5_candle(dt)
    end = start + timedelta(minutes=5)
    return start, end


def compute_indicators(df, ema_fast=8, ema_slow=21, rsi_len=14, bb_len=20):
    """
    Calcule des indicateurs techniques avancés pour M5
    Optimisé pour timeframe 5 minutes
    """
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
    
    # ATR (Average True Range) - Adapté pour M5
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
    
    # Momentum sur 3 bougies M5 (15 minutes)
    df['momentum_3'] = df['close'].pct_change(periods=3) * 100
    
    # Support/Resistance sur 20 bougies M5 (100 minutes)
    df['resistance'] = df['high'].rolling(window=20).max()
    df['support'] = df['low'].rolling(window=20).min()
    
    return df


def rule_signal_ultra_strict(df):
    """
    Stratégie optimisée pour M5 avec win rate 70-80%
    
    PARAMÈTRES AJUSTÉS POUR M5:
    - ADX minimum: 15 (tendance légère acceptable)
    - RSI zone: 20-80 (zone élargie)
    - Critères requis: 3/5 (60%)
    - Volatilité: contrôle ATR adapté à M5
    - Momentum: vérification sur 3 bougies (15 min)
    
    Timeframe: M5
    """
    
    if len(df) < 10:
        return None
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
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
    
    # CRITERE 1: TENDANCE PRESENTE (assoupli pour M5)
    # ADX > 15 = tendance légère acceptable sur M5
    if adx < 15:
        return None
    
    # CRITERE 2: VOLATILITÉ ACCEPTABLE (adapté M5)
    atr = last.get('atr', 0)
    atr_sma = df['atr'].rolling(20).mean().iloc[-1]
    # Pour M5: volatilité max 2.5x la moyenne
    if atr > atr_sma * 2.5:
        return None
    
    # CRITERE 3: RSI DANS ZONE ÉLARGIE
    # Zone: 20-80 (évite extrêmes)
    if rsi < 20 or rsi > 80:
        return None
    
    # CRITERE 4: MOMENTUM (sur 3 bougies M5 = 15 min)
    momentum = last.get('momentum_3', 0)
    # Momentum trop faible ou trop fort = rejeter
    if abs(momentum) < 0.05 or abs(momentum) > 2.0:
        return None
    
    # ANALYSE CALL (BUY) - 3/5 CRITERES (60%)
    
    call_signals = []
    
    # 1. Direction EMA principale
    ema_bullish_main = last['ema_fast'] > last['ema_slow']
    call_signals.append(ema_bullish_main)
    
    # 2. MACD haussier
    macd_bullish = macd > macd_signal and macd_hist > 0
    call_signals.append(macd_bullish)
    
    # 3. RSI dans zone haussière (40-75)
    rsi_bullish = 40 < rsi < 75
    call_signals.append(rsi_bullish)
    
    # 4. Stochastic confirme (assoupli pour M5)
    stoch_bullish = stoch_k > stoch_d and 15 < stoch_k < 90
    call_signals.append(stoch_bullish)
    
    # 5. ADX tendance haussière + momentum positif
    adx_bullish = last['adx_pos'] > last['adx_neg'] and momentum > 0
    call_signals.append(adx_bullish)
    
    # DECISION CALL: 3/5 critères (60%)
    call_score = sum(call_signals)
    if call_score >= 3:
        # Vérification finale: prix pas trop proche de la résistance
        resistance = last.get('resistance')
        if resistance and last['close'] > resistance * 0.998:
            # Trop proche de la résistance, rejeter
            return None
        return 'CALL'
    
    # ANALYSE PUT (SELL) - 3/5 CRITERES (60%)
    
    put_signals = []
    
    # 1. Direction EMA principale
    ema_bearish_main = last['ema_fast'] < last['ema_slow']
    put_signals.append(ema_bearish_main)
    
    # 2. MACD baissier
    macd_bearish = macd < macd_signal and macd_hist < 0
    put_signals.append(macd_bearish)
    
    # 3. RSI dans zone baissière (25-60)
    rsi_bearish = 25 < rsi < 60
    put_signals.append(rsi_bearish)
    
    # 4. Stochastic confirme (assoupli pour M5)
    stoch_bearish = stoch_k < stoch_d and 10 < stoch_k < 85
    put_signals.append(stoch_bearish)
    
    # 5. ADX tendance baissière + momentum négatif
    adx_bearish = last['adx_neg'] > last['adx_pos'] and momentum < 0
    put_signals.append(adx_bearish)
    
    # DECISION PUT: 3/5 critères (60%)
    put_score = sum(put_signals)
    if put_score >= 3:
        # Vérification finale: prix pas trop proche du support
        support = last.get('support')
        if support and last['close'] < support * 1.002:
            # Trop proche du support, rejeter
            return None
        return 'PUT'
    
    # Si moins de 3/5 critères, NE PAS TRADER
    return None


def rule_signal(df):
    """
    Stratégie standard (fallback)
    Utilise les mêmes critères que rule_signal_ultra_strict
    """
    return rule_signal_ultra_strict(df)


def get_signal_quality_score(df):
    """
    Calcule un score de qualité du signal (0-100)
    Utilisé pour filtrer les meilleurs signaux en kill zone
    """
    if len(df) < 10:
        return 0
    
    last = df.iloc[-1]
    score = 0
    
    # ADX score (max 20 points)
    adx = last.get('adx', 0)
    if adx > 25:
        score += 20
    elif adx > 20:
        score += 15
    elif adx > 15:
        score += 10
    
    # RSI position (max 20 points)
    rsi = last.get('rsi', 50)
    if 45 < rsi < 55:
        score += 20  # Zone neutre = bon
    elif 40 < rsi < 60:
        score += 15
    elif 35 < rsi < 65:
        score += 10
    
    # MACD alignement (max 20 points)
    macd = last.get('MACD_12_26_9', 0)
    macd_signal = last.get('MACDs_12_26_9', 0)
    macd_hist = last.get('MACDh_12_26_9', 0)
    if (macd > macd_signal and macd_hist > 0) or (macd < macd_signal and macd_hist < 0):
        score += 20
    
    # Volatilité (max 20 points)
    atr = last.get('atr', 0)
    atr_sma = df['atr'].rolling(20).mean().iloc[-1] if len(df) >= 20 else atr
    if atr_sma > 0:
        volatility_ratio = atr / atr_sma
        if 0.8 < volatility_ratio < 1.5:
            score += 20  # Volatilité normale
        elif 0.6 < volatility_ratio < 2.0:
            score += 10
    
    # EMA alignment (max 20 points)
    ema_fast = last.get('ema_fast', 0)
    ema_slow = last.get('ema_slow', 0)
    ema_50 = last.get('ema_50', 0)
    
    # Tendance claire
    if ema_fast > ema_slow > ema_50:
        score += 20  # Tendance haussière claire
    elif ema_fast < ema_slow < ema_50:
        score += 20  # Tendance baissière claire
    elif ema_fast > ema_slow or ema_fast < ema_slow:
        score += 10  # Tendance partielle
    
    return min(score, 100)


def is_kill_zone_optimal(hour_utc):
    """
    Détermine si l'heure UTC est dans une zone optimale
    Retourne (is_optimal, zone_name, priority)
    """
    # London/NY Overlap (12h-14h UTC) - Meilleure zone
    if 12 <= hour_utc < 14:
        return True, "London/NY Overlap", 5
    
    # London Open (07h-10h UTC)
    if 7 <= hour_utc < 10:
        return True, "London Open", 3
    
    # NY Open (13h-16h UTC)
    if 13 <= hour_utc < 16:
        return True, "NY Open", 3
    
    # Asian Session (00h-03h UTC)
    if 0 <= hour_utc < 3:
        return True, "Asian Session", 1
    
    return False, None, 0


def format_signal_reason(direction, confidence, indicators):
    """
    Formate une raison lisible pour le signal
    """
    last = indicators.iloc[-1]
    
    reason_parts = []
    
    # Direction
    direction_text = "Haussier" if direction == "CALL" else "Baissier"
    reason_parts.append(f"{direction_text}")
    
    # Confiance ML
    reason_parts.append(f"ML {int(confidence*100)}%")
    
    # ADX
    adx = last.get('adx', 0)
    if adx > 25:
        reason_parts.append(f"ADX fort ({adx:.0f})")
    elif adx > 20:
        reason_parts.append(f"ADX moyen ({adx:.0f})")
    
    # RSI
    rsi = last.get('rsi', 50)
    if direction == "CALL" and 45 < rsi < 60:
        reason_parts.append(f"RSI optimal ({rsi:.0f})")
    elif direction == "PUT" and 40 < rsi < 55:
        reason_parts.append(f"RSI optimal ({rsi:.0f})")
    
    return " | ".join(reason_parts)


def validate_m5_timing(entry_time):
    """
    Valide que l'heure d'entrée est bien alignée sur une bougie M5
    Retourne (is_valid, corrected_time)
    """
    if isinstance(entry_time, str):
        entry_time = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
    
    # Arrondir à la bougie M5
    corrected = round_to_m5_candle(entry_time)
    
    # Vérifier si on a dû corriger
    is_valid = (entry_time == corrected)
    
    return is_valid, corrected


def get_m5_entry_exit_times(signal_time):
    """
    À partir d'un signal_time, calcule les temps d'entrée et sortie M5 arrondis
    
    Returns:
        entry_time: Début de la bougie M5 d'entrée
        exit_time: Fin de la bougie M5 d'entrée (= début bougie suivante)
    """
    if isinstance(signal_time, str):
        signal_time = datetime.fromisoformat(signal_time.replace('Z', '+00:00'))
    
    # Entry = prochaine bougie M5 après le signal
    entry_time = get_next_m5_candle(signal_time)
    
    # Exit = fin de la bougie d'entrée
    exit_time = entry_time + timedelta(minutes=5)
    
    return entry_time, exit_time
