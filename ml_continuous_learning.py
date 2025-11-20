"""
Système d'apprentissage continu pour le ML Predictor
Réentraîne automatiquement le modèle avec les nouveaux résultats
"""

import os
import json
import pickle
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from sqlalchemy import text
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

class ContinuousLearning:
    def __init__(self, engine, model_dir='models'):
        self.engine = engine
        self.model_dir = model_dir
        self.HAITI_TZ = ZoneInfo("America/Port-au-Prince")
        
        # Créer le dossier models s'il n'existe pas
        os.makedirs(model_dir, exist_ok=True)
        
        # Chemins des fichiers
        self.model_path = os.path.join(model_dir, 'ml_model.pkl')
        self.history_path = os.path.join(model_dir, 'training_history.json')
        self.backup_dir = os.path.join(model_dir, 'backups')
        
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Historique d'entraînement
        self.training_history = self.load_training_history()
    
    def load_training_history(self):
        """Charge l'historique des entraînements"""
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            'trainings': [],
            'best_accuracy': 0.0,
            'total_signals_trained': 0
        }
    
    def save_training_history(self):
        """Sauvegarde l'historique"""
        with open(self.history_path, 'w') as f:
            json.dump(self.training_history, f, indent=2)
    
    def get_verified_signals(self, min_signals=50):
        """
        Récupère tous les signaux vérifiés (WIN/LOSE) de la base
        
        Args:
            min_signals: Nombre minimum de signaux requis pour l'entraînement
        
        Returns:
            DataFrame avec les features et résultats, ou None si insuffisant
        """
        try:
            query = text("""
                SELECT 
                    pair,
                    direction,
                    confidence,
                    result,
                    gale_level,
                    ts_enter,
                    payload_json
                FROM signals
                WHERE result IN ('WIN', 'LOSE')
                ORDER BY ts_enter ASC
            """)
            
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            print(f"📊 Signaux vérifiés trouvés: {len(df)}")
            
            if len(df) < min_signals:
                print(f"⚠️ Pas assez de signaux ({len(df)} < {min_signals})")
                return None
            
            # Extraire les features du payload_json
            if 'payload_json' in df.columns:
                df['payload'] = df['payload_json'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else {}
                )
            
            # Créer les features
            df['direction_encoded'] = df['direction'].map({'CALL': 1, 'PUT': 0})
            df['pair_encoded'] = df['pair'].astype('category').cat.codes
            df['hour'] = pd.to_datetime(df['ts_enter']).dt.hour
            df['result_binary'] = df['result'].map({'WIN': 1, 'LOSE': 0})
            
            # Features finales
            feature_cols = ['direction_encoded', 'pair_encoded', 'confidence', 'hour']
            
            # Vérifier que toutes les colonnes existent
            missing_cols = [col for col in feature_cols if col not in df.columns]
            if missing_cols:
                print(f"❌ Colonnes manquantes: {missing_cols}")
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ Erreur get_verified_signals: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def prepare_training_data(self, df):
        """
        Prépare les données pour l'entraînement
        
        Args:
            df: DataFrame avec les signaux
        
        Returns:
            X_train, X_test, y_train, y_test
        """
        try:
            feature_cols = ['direction_encoded', 'pair_encoded', 'confidence', 'hour']
            
            X = df[feature_cols].values
            y = df['result_binary'].values
            
            # Split 80/20
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            
            print(f"📊 Train: {len(X_train)} | Test: {len(X_test)}")
            print(f"📊 WIN rate train: {y_train.mean()*100:.1f}%")
            print(f"📊 WIN rate test: {y_test.mean()*100:.1f}%")
            
            return X_train, X_test, y_train, y_test
            
        except Exception as e:
            print(f"❌ Erreur prepare_training_data: {e}")
            return None, None, None, None
    
    def train_new_model(self, X_train, y_train, X_test, y_test):
        """
        Entraîne un nouveau modèle RandomForest
        
        Returns:
            model, accuracy, report
        """
        try:
            print("\n🤖 Entraînement du modèle...")
            
            # Créer le modèle
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1
            )
            
            # Entraîner
            model.fit(X_train, y_train)
            
            # Prédire sur test
            y_pred = model.predict(X_test)
            
            # Métriques
            accuracy = accuracy_score(y_test, y_pred)
            report = classification_report(y_test, y_pred, output_dict=True)
            
            print(f"✅ Accuracy: {accuracy*100:.2f}%")
            print(f"✅ Precision WIN: {report['1']['precision']*100:.2f}%")
            print(f"✅ Recall WIN: {report['1']['recall']*100:.2f}%")
            
            return model, accuracy, report
            
        except Exception as e:
            print(f"❌ Erreur train_new_model: {e}")
            import traceback
            traceback.print_exc()
            return None, 0.0, {}
    
    def save_model(self, model, accuracy, backup=True):
        """
        Sauvegarde le modèle
        
        Args:
            model: Le modèle entraîné
            accuracy: Précision du modèle
            backup: Si True, crée une sauvegarde avec timestamp
        """
        try:
            # Sauvegarder le modèle actuel
            with open(self.model_path, 'wb') as f:
                pickle.dump(model, f)
            
            print(f"💾 Modèle sauvegardé: {self.model_path}")
            
            # Créer un backup avec timestamp
            if backup:
                now = datetime.now(self.HAITI_TZ)
                backup_name = f"model_{now.strftime('%Y%m%d_%H%M%S')}_acc{accuracy:.3f}.pkl"
                backup_path = os.path.join(self.backup_dir, backup_name)
                
                with open(backup_path, 'wb') as f:
                    pickle.dump(model, f)
                
                print(f"💾 Backup créé: {backup_name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erreur save_model: {e}")
            return False
    
    def retrain_model(self, min_signals=50, min_accuracy_improvement=0.01):
        """
        Réentraîne le modèle avec les nouveaux signaux
        
        Args:
            min_signals: Nombre minimum de signaux pour réentraîner
            min_accuracy_improvement: Amélioration minimale requise pour remplacer le modèle
        
        Returns:
            dict avec les résultats de l'entraînement
        """
        try:
            print("\n" + "="*60)
            print("🔄 RÉENTRAÎNEMENT DU MODÈLE ML")
            print("="*60)
            
            now_haiti = datetime.now(self.HAITI_TZ)
            
            # 1. Récupérer les signaux vérifiés
            df = self.get_verified_signals(min_signals)
            
            if df is None:
                return {
                    'success': False,
                    'reason': 'Pas assez de signaux',
                    'signals_count': 0
                }
            
            # 2. Préparer les données
            X_train, X_test, y_train, y_test = self.prepare_training_data(df)
            
            if X_train is None:
                return {
                    'success': False,
                    'reason': 'Erreur préparation données',
                    'signals_count': len(df)
                }
            
            # 3. Entraîner le nouveau modèle
            new_model, new_accuracy, report = self.train_new_model(
                X_train, y_train, X_test, y_test
            )
            
            if new_model is None:
                return {
                    'success': False,
                    'reason': 'Erreur entraînement',
                    'signals_count': len(df)
                }
            
            # 4. Comparer avec le meilleur modèle précédent
            best_accuracy = self.training_history.get('best_accuracy', 0.0)
            improvement = new_accuracy - best_accuracy
            
            print(f"\n📊 Comparaison:")
            print(f"   Meilleur précédent: {best_accuracy*100:.2f}%")
            print(f"   Nouveau modèle: {new_accuracy*100:.2f}%")
            print(f"   Amélioration: {improvement*100:+.2f}%")
            
            # 5. Décider si on garde le nouveau modèle
            if improvement >= min_accuracy_improvement or best_accuracy == 0.0:
                print(f"\n✅ Nouveau modèle accepté (amélioration >= {min_accuracy_improvement*100:.1f}%)")
                
                # Sauvegarder
                self.save_model(new_model, new_accuracy, backup=True)
                
                # Mettre à jour l'historique
                training_entry = {
                    'timestamp': now_haiti.isoformat(),
                    'signals_count': len(df),
                    'accuracy': new_accuracy,
                    'precision_win': report['1']['precision'],
                    'recall_win': report['1']['recall'],
                    'improvement': improvement,
                    'accepted': True
                }
                
                self.training_history['trainings'].append(training_entry)
                self.training_history['best_accuracy'] = new_accuracy
                self.training_history['total_signals_trained'] = len(df)
                self.training_history['last_training'] = now_haiti.isoformat()
                
                self.save_training_history()
                
                return {
                    'success': True,
                    'accepted': True,
                    'signals_count': len(df),
                    'accuracy': new_accuracy,
                    'improvement': improvement,
                    'reason': 'Modèle amélioré'
                }
            
            else:
                print(f"\n⚠️ Nouveau modèle rejeté (amélioration trop faible)")
                
                training_entry = {
                    'timestamp': now_haiti.isoformat(),
                    'signals_count': len(df),
                    'accuracy': new_accuracy,
                    'improvement': improvement,
                    'accepted': False
                }
                
                self.training_history['trainings'].append(training_entry)
                self.save_training_history()
                
                return {
                    'success': True,
                    'accepted': False,
                    'signals_count': len(df),
                    'accuracy': new_accuracy,
                    'improvement': improvement,
                    'reason': 'Amélioration insuffisante'
                }
        
        except Exception as e:
            print(f"❌ Erreur retrain_model: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'reason': f'Erreur: {str(e)}',
                'signals_count': 0
            }
    
    def get_training_stats(self):
        """Retourne les statistiques d'entraînement"""
        return {
            'total_trainings': len(self.training_history.get('trainings', [])),
            'best_accuracy': self.training_history.get('best_accuracy', 0.0),
            'total_signals': self.training_history.get('total_signals_trained', 0),
            'last_training': self.training_history.get('last_training', 'Jamais'),
            'recent_trainings': self.training_history.get('trainings', [])[-5:]  # 5 derniers
        }


# === Fonction pour intégration dans le bot ===

async def scheduled_retraining(engine, telegram_app=None, admin_chat_ids=None):
    """
    Fonction appelée automatiquement chaque nuit pour réentraîner le modèle
    
    Args:
        engine: SQLAlchemy engine
        telegram_app: Application Telegram (optionnel, pour notifier)
        admin_chat_ids: Liste des IDs admin à notifier (optionnel)
    """
    try:
        print("\n🌙 Réentraînement nocturne programmé...")
        
        learner = ContinuousLearning(engine)
        
        # Réentraîner avec minimum 50 signaux
        result = learner.retrain_model(min_signals=50, min_accuracy_improvement=0.01)
        
        # Créer le message de notification
        if result['success']:
            if result['accepted']:
                emoji = "✅"
                status = "ACCEPTÉ"
                msg = (
                    f"{emoji} **Réentraînement ML {status}**\n\n"
                    f"📊 Signaux utilisés: {result['signals_count']}\n"
                    f"🎯 Accuracy: {result['accuracy']*100:.2f}%\n"
                    f"📈 Amélioration: {result['improvement']*100:+.2f}%\n"
                    f"✨ {result['reason']}"
                )
            else:
                emoji = "⚠️"
                status = "REJETÉ"
                msg = (
                    f"{emoji} **Réentraînement ML {status}**\n\n"
                    f"📊 Signaux utilisés: {result['signals_count']}\n"
                    f"🎯 Accuracy: {result['accuracy']*100:.2f}%\n"
                    f"📉 Amélioration: {result['improvement']*100:+.2f}%\n"
                    f"ℹ️ {result['reason']}"
                )
        else:
            emoji = "❌"
            msg = (
                f"{emoji} **Réentraînement ML ÉCHOUÉ**\n\n"
                f"❌ {result['reason']}\n"
                f"📊 Signaux disponibles: {result['signals_count']}"
            )
        
        print(msg)
        
        # Envoyer notification aux admins si configuré
        if telegram_app and admin_chat_ids:
            for admin_id in admin_chat_ids:
                try:
                    await telegram_app.bot.send_message(chat_id=admin_id, text=msg)
                except Exception as e:
                    print(f"❌ Erreur envoi notification à {admin_id}: {e}")
        
        return result
        
    except Exception as e:
        print(f"❌ Erreur scheduled_retraining: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'reason': str(e)}
