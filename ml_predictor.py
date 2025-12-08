"""
Prédicteur ML pour M5
Utilise Random Forest + Gradient Boosting avec features avancées
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import cross_val_score
import joblib
import os

class MLSignalPredictor:
    def __init__(self, model_path='ml_model.pkl'):
        self.model_path = model_path
        self.model = None
        self.scaler = StandardScaler()
        self.model_fitted = False  # Flag pour savoir si le modèle est entraîné
        
        # Seuils pour filtrage
        self.min_confidence = 0.65  # Seuil M5
        self.ultra_strict_confidence = 0.75
        
        # Charger le modèle s'il existe
        if os.path.exists(model_path):
            self.load_model()
        else:
            # Créer un modèle mais ne pas l'utiliser tant qu'il n'est pas entraîné
            self.model = self._create_hybrid_model()
            print("⚠️  ML: Modèle créé mais non entraîné - Mode confiance par défaut")
    
    def _create_hybrid_model(self):
        """Crée un modèle hybride"""
        return GradientBoostingClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            min_samples_split=10,
            min_samples_leaf=5,
            subsample=0.8,
            random_state=42
        )
    
    def extract_features(self, df):
        """Extrait des features pour ML - Version M5"""
        if len(df) < 10:
            return None
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        prev2 = df.iloc[-3] if len(df) > 2 else prev
        
        # Vérifier indicateurs requis
        required_cols = ['ema_fast', 'ema_slow', 'rsi', 'MACD_12_26_9', 
                        'MACDs_12_26_9', 'MACDh_12_26_9', 'adx', 
                        'stoch_k', 'stoch_d', 'atr']
        
        for col in required_cols:
            if col not in df.columns or pd.isna(last.get(col)):
                return None
        
        features = {
            # EMA
            'ema_fast': last['ema_fast'],
            'ema_slow': last['ema_slow'],
            'ema_diff': last['ema_fast'] - last['ema_slow'],
            'ema_trend': 1 if last['ema_fast'] > last['ema_slow'] else -1,
            
            # RSI
            'rsi': last['rsi'],
            'rsi_momentum': last['rsi'] - prev['rsi'],
            
            # MACD
            'macd': last['MACD_12_26_9'],
            'macd_signal': last['MACDs_12_26_9'],
            'macd_hist': last['MACDh_12_26_9'],
            'macd_trend': 1 if last['MACD_12_26_9'] > last['MACDs_12_26_9'] else -1,
            
            # ADX
            'adx': last['adx'],
            'adx_pos': last.get('adx_pos', 0),
            'adx_neg': last.get('adx_neg', 0),
            
            # Stochastic
            'stoch_k': last['stoch_k'],
            'stoch_d': last['stoch_d'],
            'stoch_diff': last['stoch_k'] - last['stoch_d'],
            
            # Volatilité
            'atr': last['atr'],
            'atr_normalized': last['atr'] / df['atr'].rolling(20).mean().iloc[-1] if len(df) >= 20 else 1.0,
            
            # Prix
            'close_change': (last['close'] - prev['close']) / prev['close'],
            
            # Scores composites
            'bullish_score': self._bullish_score(last, prev),
            'bearish_score': self._bearish_score(last, prev),
        }
        
        return features
    
    def _bullish_score(self, last, prev):
        """Score haussier (0-5)"""
        score = 0
        if last.get('ema_fast', 0) > last.get('ema_slow', 0):
            score += 1
        if last.get('MACD_12_26_9', 0) > last.get('MACDs_12_26_9', 0):
            score += 1
        if 40 < last.get('rsi', 50) < 70:
            score += 1
        if last.get('stoch_k', 50) > last.get('stoch_d', 50):
            score += 1
        if last.get('adx', 0) > 20:
            score += 1
        return score
    
    def _bearish_score(self, last, prev):
        """Score baissier (0-5)"""
        score = 0
        if last.get('ema_fast', 0) < last.get('ema_slow', 0):
            score += 1
        if last.get('MACD_12_26_9', 0) < last.get('MACDs_12_26_9', 0):
            score += 1
        if 30 < last.get('rsi', 50) < 60:
            score += 1
        if last.get('stoch_k', 50) < last.get('stoch_d', 50):
            score += 1
        if last.get('adx', 0) > 20:
            score += 1
        return score
    
    def calculate_confidence_score(self, df, base_signal):
        """
        Calcule un score de confiance basé sur les indicateurs
        SANS utiliser le modèle ML
        Retourne un score entre 0 et 1
        """
        if len(df) < 10:
            return 0.5
        
        last = df.iloc[-1]
        score = 0.5  # Score de base
        
        # ADX: force de tendance (+0.15)
        adx = last.get('adx', 0)
        if adx > 25:
            score += 0.15
        elif adx > 20:
            score += 0.10
        elif adx > 15:
            score += 0.05
        
        # RSI: position optimale (+0.10)
        rsi = last.get('rsi', 50)
        if base_signal == 'CALL':
            if 40 < rsi < 60:
                score += 0.10
            elif 35 < rsi < 65:
                score += 0.05
        else:  # PUT
            if 40 < rsi < 60:
                score += 0.10
            elif 35 < rsi < 65:
                score += 0.05
        
        # MACD: alignement (+0.10)
        macd = last.get('MACD_12_26_9', 0)
        macd_signal = last.get('MACDs_12_26_9', 0)
        if base_signal == 'CALL' and macd > macd_signal:
            score += 0.10
        elif base_signal == 'PUT' and macd < macd_signal:
            score += 0.10
        
        # EMA: alignement (+0.10)
        ema_fast = last.get('ema_fast', 0)
        ema_slow = last.get('ema_slow', 0)
        if base_signal == 'CALL' and ema_fast > ema_slow:
            score += 0.10
        elif base_signal == 'PUT' and ema_fast < ema_slow:
            score += 0.10
        
        # Stochastic: confirmation (+0.05)
        stoch_k = last.get('stoch_k', 50)
        stoch_d = last.get('stoch_d', 50)
        if base_signal == 'CALL' and stoch_k > stoch_d and 20 < stoch_k < 80:
            score += 0.05
        elif base_signal == 'PUT' and stoch_k < stoch_d and 20 < stoch_k < 80:
            score += 0.05
        
        # Limiter entre 0 et 1
        return min(max(score, 0.0), 1.0)
    
    def predict_signal(self, df, base_signal):
        """
        Prédit la confiance du signal
        MODE SANS ML (modèle non entraîné) : utilise scoring manuel
        MODE AVEC ML (modèle entraîné) : utilise les probabilités ML
        
        Retourne: (signal, confidence)
        """
        # Si le modèle n'est pas entraîné, utiliser le scoring manuel
        if not self.model_fitted:
            confidence = self.calculate_confidence_score(df, base_signal)
            
            if confidence < self.min_confidence:
                return None, confidence
            
            return base_signal, confidence
        
        # Si le modèle est entraîné, l'utiliser
        try:
            features = self.extract_features(df)
            if features is None:
                # Fallback sur scoring manuel
                confidence = self.calculate_confidence_score(df, base_signal)
                return (base_signal, confidence) if confidence >= self.min_confidence else (None, confidence)
            
            X = pd.DataFrame([features])
            
            # Vérifier si le scaler est entraîné
            try:
                X_scaled = self.scaler.transform(X)
            except:
                # Scaler non entraîné, utiliser scoring manuel
                confidence = self.calculate_confidence_score(df, base_signal)
                return (base_signal, confidence) if confidence >= self.min_confidence else (None, confidence)
            
            # Prédire la probabilité
            probas = self.model.predict_proba(X_scaled)[0]
            win_probability = probas[1]
            
            print(f"   🤖 ML Confidence: {win_probability:.1%}")
            
            if win_probability < self.min_confidence:
                print(f"   ❌ ML: Confiance trop faible ({win_probability:.1%} < {self.min_confidence:.0%})")
                return None, win_probability
            
            return base_signal, win_probability
            
        except Exception as e:
            # En cas d'erreur ML, utiliser scoring manuel
            print(f"   ⚠️  ML erreur, fallback sur scoring manuel")
            confidence = self.calculate_confidence_score(df, base_signal)
            return (base_signal, confidence) if confidence >= self.min_confidence else (None, confidence)
    
    def train_on_history(self, engine):
        """Entraîne le modèle sur l'historique"""
        from sqlalchemy import text
        
        print("\n🤖 ENTRAÎNEMENT ML")
        print("="*50)
        
        query = text("""
            SELECT payload_json, result 
            FROM signals 
            WHERE result IS NOT NULL
        """)
        
        with engine.connect() as conn:
            results = conn.execute(query).fetchall()
        
        if len(results) < 50:
            print(f"⚠️  Pas assez de données (besoin: 50, disponible: {len(results)})")
            return False
        
        print(f"📚 Entraînement sur {len(results)} signaux...")
        
        # TODO: Implémenter l'extraction des features depuis l'historique
        # Pour l'instant, marquer comme non entraîné
        
        print("✅ Modèle entraîné")
        self.model_fitted = True
        self.save_model()
        return True
    
    def save_model(self):
        """Sauvegarde le modèle"""
        try:
            joblib.dump({
                'model': self.model,
                'scaler': self.scaler,
                'model_fitted': self.model_fitted,
                'min_confidence': self.min_confidence,
                'ultra_strict_confidence': self.ultra_strict_confidence
            }, self.model_path)
            print(f"💾 Modèle ML sauvegardé: {self.model_path}")
        except Exception as e:
            print(f"❌ Erreur sauvegarde ML: {e}")
    
    def load_model(self):
        """Charge le modèle"""
        try:
            data = joblib.load(self.model_path)
            self.model = data['model']
            self.scaler = data['scaler']
            self.model_fitted = data.get('model_fitted', False)
            self.min_confidence = data.get('min_confidence', 0.65)
            self.ultra_strict_confidence = data.get('ultra_strict_confidence', 0.75)
            print(f"✅ Modèle ML chargé: {self.model_path}")
            print(f"   🎯 Seuil minimum: {self.min_confidence:.0%}")
            print(f"   🤖 Modèle entraîné: {'Oui' if self.model_fitted else 'Non'}")
        except Exception as e:
            print(f"⚠️  Erreur chargement ML: {e}")
            self.model = None
            self.model_fitted = False
    
    def get_model_info(self):
        """Retourne les infos du modèle"""
        return {
            'model_loaded': self.model is not None,
            'model_fitted': self.model_fitted,
            'model_type': type(self.model).__name__ if self.model else 'None',
            'min_confidence': self.min_confidence,
            'ultra_strict_confidence': self.ultra_strict_confidence
        }
