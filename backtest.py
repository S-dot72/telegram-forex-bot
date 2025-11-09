import pandas as pd
import numpy as np
import os
import json
import requests
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, precision_score, recall_score
from dotenv import load_dotenv

from utils import rule_signal
from ml_predictor import MLSignalPredictor

load_dotenv()

# --------------------------------------------
# Indicateurs techniques paramétrables
# --------------------------------------------
def compute_indicators(df, ema_fast=8, ema_slow=21, rsi_period=14, bb_period=20, bb_std=2):
    df['ema_fast'] = df['close'].ewm(span=ema_fast, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=ema_slow, adjust=False).mean()

    # RSI
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(rsi_period).mean()
    avg_loss = loss.rolling(rsi_period).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))

    # Bollinger Bands
    df['bb_mid'] = df['close'].rolling(bb_period).mean()
    df['bb_std'] = df['close'].rolling(bb_period).std()
    df['bb_upper'] = df['bb_mid'] + bb_std * df['bb_std']
    df['bb_lower'] = df['bb_mid'] - bb_std * df['bb_std']

    df.fillna(method='bfill', inplace=True)
    return df

# --------------------------------------------
# Backtester ML
# --------------------------------------------
class MLBacktester:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://api.twelvedata.com/time_series'
        self.ml_predictor = MLSignalPredictor()

    # ----------------------------------------
    # Récupération des données
    # ----------------------------------------
    def fetch_historical_data(self, pair, days=90):
        outputsize = min(days * 1440, 5000)
        params = {
            'symbol': pair,
            'interval': '1min',
            'outputsize': outputsize,
            'apikey': self.api_key,
            'format': 'JSON'
        }
        response = requests.get(self.base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if 'values' not in data:
            raise RuntimeError(f"Erreur API: {data}")
        df = pd.DataFrame(data['values'])[::-1].reset_index(drop=True)
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col].astype(float)
        if 'volume' in df.columns:
            df['volume'] = df['volume'].astype(float)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        return df

    # ----------------------------------------
    # Simulation d’un trade
    # ----------------------------------------
    def simulate_trade(self, df, entry_idx, direction, duration=5):
        if entry_idx + duration >= len(df):
            return None, None
        entry_price = df.iloc[entry_idx]['close']
        exit_price = df.iloc[entry_idx + duration]['close']
        if direction == 'CALL':
            result = 'WIN' if exit_price > entry_price else 'LOSE'
        else:
            result = 'WIN' if exit_price < entry_price else 'LOSE'
        pips = abs(exit_price - entry_price) * 10000
        return result, pips

    # ----------------------------------------
    # Backtesting
    # ----------------------------------------
    def backtest_strategy(self, pair, days=30, ema_fast=8, ema_slow=21, rsi_period=14):
        df = self.fetch_historical_data(pair, days)
        df = compute_indicators(df, ema_fast, ema_slow, rsi_period)

        features_list, labels_list, signals_data = [], [], []

        for i in range(200, len(df) - 10):
            df_slice = df.iloc[:i+1].copy()
            signal = rule_signal(df_slice)
            if signal:
                result, pips = self.simulate_trade(df, i, signal)
                if result:
                    features = self.ml_predictor.extract_features(df_slice)
                    features_list.append(features)
                    labels_list.append(1 if result == 'WIN' else 0)
                    signals_data.append({
                        'index': i, 'signal': signal, 'result': result, 'pips': pips,
                        'entry_price': df.iloc[i]['close'],
                        'exit_price': df.iloc[i+5]['close']
                    })

        if not signals_data:
            print("❌ Aucun signal généré")
            return None

        # Stats
        total_signals = len(signals_data)
        wins = sum(1 for s in signals_data if s['result'] == 'WIN')
        losses = total_signals - wins
        winrate = (wins / total_signals) * 100
        avg_win_pips = np.mean([s['pips'] for s in signals_data if s['result'] == 'WIN']) if wins else 0
        avg_loss_pips = np.mean([s['pips'] for s in signals_data if s['result'] == 'LOSE']) if losses else 0

        # ML training
        if len(features_list) >= 50:
            self.train_ml_model(features_list, labels_list)

        return {
            'pair': pair, 'total_signals': total_signals, 'wins': wins, 'losses': losses,
            'winrate': winrate, 'avg_win_pips': avg_win_pips, 'avg_loss_pips': avg_loss_pips,
            'signals': signals_data
        }

    # ----------------------------------------
    # Entraînement ML
    # ----------------------------------------
    def train_ml_model(self, features_list, labels_list):
        X = pd.DataFrame(features_list).fillna(0)
        y = np.array(labels_list)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        self.ml_predictor.scaler.fit(X_train)
        X_train_scaled = self.ml_predictor.scaler.transform(X_train)
        X_test_scaled = self.ml_predictor.scaler.transform(X_test)

        self.ml_predictor.model = RandomForestClassifier(
            n_estimators=100, max_depth=10, min_samples_split=5, random_state=42
        )
        self.ml_predictor.model.fit(X_train_scaled, y_train)

        y_pred = self.ml_predictor.model.predict(X_test_scaled)
        print(f"🎯 Accuracy: {accuracy_score(y_test, y_pred):.1%}, "
              f"Precision: {precision_score(y_test, y_pred, zero_division=0):.1%}, "
              f"Recall: {recall_score(y_test, y_pred, zero_division=0):.1%}")

        self.ml_predictor.save_model()

    # ----------------------------------------
    # Optimisation des paramètres
    # ----------------------------------------
    def optimize_parameters(self, pair, param_ranges, days=14):
        df = self.fetch_historical_data(pair, days)
        best_params, best_winrate = None, 0

        for ema_fast in param_ranges['ema_fast']:
            for ema_slow in param_ranges['ema_slow']:
                for rsi in param_ranges['rsi']:
                    df_ind = compute_indicators(df.copy(), ema_fast, ema_slow, rsi)
                    features_list, labels_list, signals_data = [], [], []

                    for i in range(200, len(df_ind)-10):
                        df_slice = df_ind.iloc[:i+1].copy()
                        signal = rule_signal(df_slice)
                        if signal:
                            result, pips = self.simulate_trade(df_ind, i, signal)
                            if result:
                                features_list.append(self.ml_predictor.extract_features(df_slice))
                                labels_list.append(1 if result=='WIN' else 0)
                                signals_data.append({'result': result})

                    if signals_data:
                        total_signals = len(signals_data)
                        wins = sum(1 for s in signals_data if s['result']=='WIN')
                        winrate = (wins/total_signals)*100
                        if winrate > best_winrate:
                            best_winrate = winrate
                            best_params = {'ema_fast': ema_fast, 'ema_slow': ema_slow, 'rsi': rsi, 'bb': 20}
                        print(f"Test EMA({ema_fast},{ema_slow}), RSI({rsi}) => Winrate: {winrate:.1f}%")
                    else:
                        print(f"Test EMA({ema_fast},{ema_slow}), RSI({rsi}) => Aucun signal")

        print(f"🏆 Meilleurs paramètres: {best_params} avec winrate {best_winrate:.1f}%")
        return best_params, best_winrate

# --------------------------------------------
# Main
# --------------------------------------------
def main():
    api_key = os.getenv('TWELVEDATA_API_KEY')
    if not api_key:
        print("❌ TWELVEDATA_API_KEY non trouvée!")
        return

    backtester = MLBacktester(api_key)

    pairs = ['EUR/USD', 'GBP/USD']
    param_ranges = {
        'ema_fast': [5, 8, 10],
        'ema_slow': [20, 21, 30],
        'rsi': [10, 14, 20]
    }

    all_results = {}
    for pair in pairs:
        results = backtester.backtest_strategy(pair, days=30)
        if results:
            all_results[pair] = results
            best_params, best_winrate = backtester.optimize_parameters(pair, param_ranges, days=14)
            print(f"✅ {pair} Best Params: {best_params}, Winrate: {best_winrate:.1f}%")

    # Sauvegarder les meilleurs paramètres
    with open('best_params.json', 'w') as f:
        json.dump({pair: r.get('winrate') for pair,r in all_results.items()}, f, indent=2)

    print("✅ Backtesting terminé!")

if __name__ == '__main__':
    main()
