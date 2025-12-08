"""
Backtester M5 avec envoi des résultats sur Telegram
"""

import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from utils import compute_indicators, rule_signal_ultra_strict, get_signal_quality_score
from ml_predictor import MLSignalPredictor
from config import TWELVEDATA_API_KEY, PAIRS, TIMEFRAME_M5

TD = 'https://api.twelvedata.com/time_series'

class BacktesterM5:
    def __init__(self, confidence_threshold=0.65):
        self.confidence_threshold = confidence_threshold
        self.ml_predictor = MLSignalPredictor()
    
    def generate_demo_data(self, pair, num_candles=1000):
        """Génère des données OHLC réalistes pour démo week-end"""
        import numpy as np
        from datetime import datetime, timedelta, timezone
        
        print(f"   🎭 Génération de {num_candles} bougies de démo...")
        
        # Prix de base selon la paire
        base_prices = {
            'EUR/USD': 1.0850,
            'GBP/USD': 1.2650,
            'USD/JPY': 149.50,
            'BTC/USD': 42000.0,
            'ETH/USD': 2200.0,
            'XRP/USD': 0.60
        }
        
        base_price = base_prices.get(pair, 1.0)
        
        # Générer des dates (7 jours en arrière)
        end_time = datetime.now(timezone.utc) - timedelta(days=2)
        dates = [end_time - timedelta(minutes=5*i) for i in range(num_candles)]
        dates.reverse()
        
        # Générer des prix avec tendance et volatilité réalistes
        np.random.seed(42)  # Pour reproductibilité
        
        prices = [base_price]
        for i in range(1, num_candles):
            # Mouvement aléatoire avec tendance légère
            change_pct = np.random.normal(0, 0.0003)  # 0.03% volatilité moyenne
            new_price = prices[-1] * (1 + change_pct)
            prices.append(new_price)
        
        # Créer DataFrame
        data = []
        for i, (date, close) in enumerate(zip(dates, prices)):
            # OHLC avec variations réalistes
            volatility = close * 0.0002
            high = close + np.random.uniform(0, volatility)
            low = close - np.random.uniform(0, volatility)
            open_price = prices[i-1] if i > 0 else close
            
            data.append({
                'datetime': date.strftime('%Y-%m-%d %H:%M:%S'),
                'open': round(open_price, 5),
                'high': round(high, 5),
                'low': round(low, 5),
                'close': round(close, 5),
                'volume': int(np.random.uniform(1000, 5000))
            })
        
        df = pd.DataFrame(data)
        df.index = pd.to_datetime(df['datetime'])
        
        print(f"   ✅ Données de démo générées")
        return df
    
    def fetch_historical_data(self, pair, interval='5min', outputsize=10000):
        """Récupère les données historiques M5"""
        try:
            from datetime import datetime, timedelta, timezone
            now_utc = datetime.now(timezone.utc)
            
            # CORRECTION: Vérifier si le marché Forex est VRAIMENT fermé
            # Forex ouvert: Dimanche 22h UTC → Vendredi 22h UTC
            weekday = now_utc.weekday()
            hour = now_utc.hour
            
            # Samedi = toujours fermé
            is_weekend = (weekday == 5)
            
            # Dimanche = fermé AVANT 22h UTC
            if weekday == 6 and hour < 22:
                is_weekend = True
            
            # Vendredi = fermé APRÈS 22h UTC
            if weekday == 4 and hour >= 22:
                is_weekend = True
            
            # Si lundi-jeudi ou dimanche après 22h = marché OUVERT
            if weekday in [0, 1, 2, 3] or (weekday == 6 and hour >= 22):
                is_weekend = False
            
            # MODE DÉMO WEEK-END
            if is_weekend:
                print(f"   🏖️ Week-end détecté - Mode DÉMO activé")
                return self.generate_demo_data(pair, num_candles=2000)
            
            # MODE NORMAL (marché ouvert)
            print(f"   📡 Marché OUVERT - Données réelles API")
            symbol = pair.replace('/', '')
            
            end_date = now_utc
            start_date = end_date - timedelta(days=7)
            
            params = {
                'symbol': symbol,
                'interval': interval,
                'apikey': TWELVEDATA_API_KEY,
                'format': 'JSON',
                'start_date': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': end_date.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            print(f"   📥 Téléchargement données M5 ({symbol})...")
            
            r = requests.get(TD, params=params, timeout=30)
            r.raise_for_status()
            j = r.json()
            
            # Vérifier les erreurs API
            if 'status' in j and j['status'] == 'error':
                print(f"   ⚠️ Erreur API: {j.get('message', 'Unknown')}")
                print(f"   🎭 Basculement en mode DÉMO")
                return self.generate_demo_data(pair, num_candles=2000)
            
            if 'values' not in j or not j['values']:
                print(f"   ⚠️ Aucune valeur retournée")
                print(f"   🔄 Tentative sans plage de dates...")
                
                params_simple = {
                    'symbol': symbol,
                    'interval': interval,
                    'outputsize': 2000,
                    'apikey': TWELVEDATA_API_KEY,
                    'format': 'JSON'
                }
                r = requests.get(TD, params=params_simple, timeout=30)
                r.raise_for_status()
                j = r.json()
                
                if 'values' not in j or not j['values']:
                    print(f"   ❌ Échec API - Basculement mode DÉMO")
                    return self.generate_demo_data(pair, num_candles=2000)
            
            df = pd.DataFrame(j['values'])[::-1].reset_index(drop=True)
            
            if len(df) == 0:
                print(f"   ❌ DataFrame vide - Mode DÉMO")
                return self.generate_demo_data(pair, num_candles=2000)
            
            for col in ['open', 'high', 'low', 'close']:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            
            if 'volume' in df.columns:
                df['volume'] = df['volume'].astype(float)
            
            df.index = pd.to_datetime(df['datetime'])
            
            print(f"   ✅ {len(df)} bougies réelles chargées")
            return df
            
        except Exception as e:
            print(f"   ❌ Erreur: {e}")
            print(f"   🎭 Basculement en mode DÉMO")
            return self.generate_demo_data(pair, num_candles=2000)
    
    def run_backtest(self, pair, outputsize=5000, use_ml=True):
        """
        Lance un backtest sur une paire
        
        Args:
            pair: Paire à tester (ex: EUR/USD)
            outputsize: Nombre de bougies à analyser
            use_ml: Utiliser le ML pour filtrer les signaux
        
        Returns:
            dict avec les résultats
        """
        print(f"\n{'='*60}")
        print(f"📊 BACKTEST M5: {pair}")
        print(f"{'='*60}")
        
        # Récupérer les données
        df = self.fetch_historical_data(pair, interval='5min', outputsize=outputsize)
        
        if df is None or len(df) < 100:
            return {
                'pair': pair,
                'error': 'Pas assez de données',
                'total_trades': 0,
                'wins': 0,
                'losses': 0,
                'winrate': 0
            }
        
        # Calculer les indicateurs
        print(f"   📈 Calcul des indicateurs techniques...")
        df = compute_indicators(df)
        
        # Variables de résultats
        trades = []
        wins = 0
        losses = 0
        total_trades = 0
        
        # Variables pour statistiques détaillées
        call_wins = 0
        call_total = 0
        put_wins = 0
        put_total = 0
        
        best_streak = 0
        worst_streak = 0
        current_streak = 0
        
        total_pips = 0
        
        print(f"   🔍 Analyse de {len(df)} bougies M5...")
        
        # Parcourir l'historique (laisser 50 bougies pour les indicateurs)
        for i in range(50, len(df) - 1):
            # Fenêtre d'analyse jusqu'à la bougie i
            window = df.iloc[:i+1]
            
            # Obtenir signal de la stratégie
            base_signal = rule_signal_ultra_strict(window)
            
            if not base_signal:
                continue
            
            # Filtrage ML si activé
            if use_ml:
                ml_signal, ml_conf = self.ml_predictor.predict_signal(window, base_signal)
                
                if ml_signal is None or ml_conf < self.confidence_threshold:
                    continue
                
                signal = ml_signal
                confidence = ml_conf
            else:
                signal = base_signal
                confidence = 0.5
            
            # Prix d'entrée et de sortie (bougie M5)
            entry_price = df['close'].iloc[i]
            exit_price = df['close'].iloc[i + 1]
            
            # Calculer le résultat
            price_diff = exit_price - entry_price
            pips = abs(price_diff) * 10000
            
            if signal == 'CALL':
                is_win = exit_price > entry_price
                call_total += 1
                if is_win:
                    call_wins += 1
            else:  # PUT
                is_win = exit_price < entry_price
                put_total += 1
                if is_win:
                    put_wins += 1
            
            # Statistiques
            total_trades += 1
            if is_win:
                wins += 1
                current_streak = current_streak + 1 if current_streak > 0 else 1
                total_pips += pips
            else:
                losses += 1
                current_streak = current_streak - 1 if current_streak < 0 else -1
                total_pips -= pips
            
            # Meilleures/pires séries
            if current_streak > best_streak:
                best_streak = current_streak
            if current_streak < worst_streak:
                worst_streak = current_streak
            
            # Enregistrer le trade
            trades.append({
                'timestamp': df.index[i],
                'signal': signal,
                'entry': entry_price,
                'exit': exit_price,
                'pips': pips,
                'result': 'WIN' if is_win else 'LOSE',
                'confidence': confidence
            })
        
        # Calculer statistiques finales
        winrate = (wins / total_trades * 100) if total_trades > 0 else 0
        call_winrate = (call_wins / call_total * 100) if call_total > 0 else 0
        put_winrate = (put_wins / put_total * 100) if put_total > 0 else 0
        avg_pips = total_pips / total_trades if total_trades > 0 else 0
        
        # Ajouter info sur la période
        if len(df) > 0:
            period_start = df.index[0].strftime('%Y-%m-%d')
            period_end = df.index[-1].strftime('%Y-%m-%d')
        else:
            period_start = "N/A"
            period_end = "N/A"
        
        results = {
            'pair': pair,
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'winrate': winrate,
            'call_trades': call_total,
            'call_wins': call_wins,
            'call_winrate': call_winrate,
            'put_trades': put_total,
            'put_wins': put_wins,
            'put_winrate': put_winrate,
            'best_streak': best_streak,
            'worst_streak': abs(worst_streak),
            'total_pips': total_pips,
            'avg_pips_per_trade': avg_pips,
            'trades': trades[-20:],  # Garder les 20 derniers trades
            'use_ml': use_ml,
            'ml_threshold': self.confidence_threshold if use_ml else None,
            'period_start': period_start,
            'period_end': period_end
        }
        
        print(f"\n   📊 Résultats:")
        print(f"   • Trades: {total_trades}")
        print(f"   • Wins: {wins} ({winrate:.1f}%)")
        print(f"   • Losses: {losses}")
        print(f"   • Total pips: {total_pips:+.1f}")
        print(f"   • Meilleure série: {best_streak}")
        print(f"   • Pire série: {abs(worst_streak)}")
        
        return results
    
    def run_full_backtest(self, pairs=None, outputsize=5000):
        """Lance le backtest sur plusieurs paires"""
        if pairs is None:
            pairs = PAIRS[:3]  # Top 3 paires par défaut
        
        all_results = []
        
        print(f"\n{'='*60}")
        print(f"🚀 BACKTEST COMPLET M5")
        print(f"{'='*60}")
        print(f"Paires: {', '.join(pairs)}")
        print(f"Bougies par paire: {outputsize}")
        print(f"ML activé: Oui (seuil {self.confidence_threshold*100:.0f}%)")
        print(f"{'='*60}\n")
        
        for pair in pairs:
            result = self.run_backtest(pair, outputsize=outputsize)
            all_results.append(result)
        
        return all_results
    
    def format_results_for_telegram(self, results):
        """Formate les résultats pour l'envoi Telegram"""
        if isinstance(results, dict):
            # Un seul résultat
            results = [results]
        
        # Calculer statistiques globales
        total_trades = sum(r['total_trades'] for r in results)
        total_wins = sum(r['wins'] for r in results)
        total_losses = sum(r['losses'] for r in results)
        global_winrate = (total_wins / total_trades * 100) if total_trades > 0 else 0
        total_pips = sum(r.get('total_pips', 0) for r in results)
        
        # Message principal
        msg = "📊 **RÉSULTATS BACKTEST M5**\n"
        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
        
        msg += f"🎯 **GLOBAL**\n"
        msg += f"• Total trades: {total_trades}\n"
        msg += f"• ✅ Wins: {total_wins} ({global_winrate:.1f}%)\n"
        msg += f"• ❌ Losses: {total_losses}\n"
        msg += f"• 💰 Total pips: {total_pips:+.1f}\n\n"
        
        msg += f"📈 **DÉTAILS PAR PAIRE**\n\n"
        
        # Afficher la période si disponible
        if results and results[0].get('period_start'):
            msg += f"📅 Période: {results[0]['period_start']} → {results[0]['period_end']}\n\n"
        
        for r in results:
            if r.get('error'):
                msg += f"❌ **{r['pair']}**: {r['error']}\n\n"
                continue
            
            msg += f"💱 **{r['pair']}**\n"
            msg += f"• Trades: {r['total_trades']}\n"
            msg += f"• Win rate: {r['winrate']:.1f}%\n"
            
            if r['call_trades'] > 0:
                msg += f"• 📈 CALL: {r['call_wins']}/{r['call_trades']} ({r['call_winrate']:.1f}%)\n"
            if r['put_trades'] > 0:
                msg += f"• 📉 PUT: {r['put_wins']}/{r['put_trades']} ({r['put_winrate']:.1f}%)\n"
            
            msg += f"• Pips: {r.get('total_pips', 0):+.1f}\n"
            msg += f"• Meilleure série: {r.get('best_streak', 0)}\n"
            msg += f"• Pire série: {r.get('worst_streak', 0)}\n\n"
        
        msg += "━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📍 Timeframe: M5\n"
        
        if results and results[0].get('use_ml'):
            msg += f"🤖 ML: Activé ({results[0]['ml_threshold']*100:.0f}%)\n"
        
        return msg


def run_quick_backtest(pair='EUR/USD', outputsize=2000):
    """Fonction rapide pour tester une paire"""
    backtester = BacktesterM5()
    result = backtester.run_backtest(pair, outputsize=outputsize)
    print("\n" + backtester.format_results_for_telegram(result))
    return result


if __name__ == '__main__':
    print("\n🚀 Lancement backtest simple...\n")
    
    backtester = BacktesterM5(confidence_threshold=0.65)
    
    # Test sur les 3 principales paires
    results = backtester.run_full_backtest(
        pairs=['EUR/USD', 'GBP/USD', 'USD/JPY'],
        outputsize=3000
    )
    
    # Afficher résumé
    print("\n" + "="*60)
    print(backtester.format_results_for_telegram(results))
    print("="*60)
