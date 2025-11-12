"""
Système de vérification automatique des résultats
Vérifie si les signaux ont gagné ou perdu en analysant les prix après l'entrée
Prend en compte le timeframe et les gales
"""

import asyncio
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
import requests

class AutoResultVerifier:
    def __init__(self, engine, twelvedata_api_key, bot=None):
        self.engine = engine
        self.api_key = twelvedata_api_key
        self.base_url = 'https://api.twelvedata.com/time_series'
        self.bot = bot
        self.admin_chat_ids = []
    
    def set_bot(self, bot):
        """Configure le bot pour les notifications"""
        self.bot = bot
    
    def add_admin(self, chat_id):
        """Ajoute un admin pour recevoir les rapports"""
        if chat_id not in self.admin_chat_ids:
            self.admin_chat_ids.append(chat_id)
            print(f"✅ Admin {chat_id} ajouté pour recevoir les rapports")
    
    async def verify_pending_signals(self):
        """
        Vérifie tous les signaux qui n'ont pas encore de résultat
        et dont toutes les tentatives (signal + gales) sont terminées
        """
        try:
            print("\n" + "="*60)
            print(f"🔍 VÉRIFICATION AUTOMATIQUE - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print("="*60)
            
            # Récupérer les signaux sans résultat - VERSION COMPATIBLE SANS COLONNES
            query = text("""
                SELECT id, pair, direction, ts_enter, confidence
                FROM signals 
                WHERE result IS NULL 
                AND datetime(ts_enter) < datetime('now')
                ORDER BY ts_enter DESC
                LIMIT 50
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query).fetchall()
                # Ajouter les valeurs par défaut manuellement
                pending = []
                for row in result:
                    # Créer un objet avec les attributs nécessaires
                    class SignalRow:
                        def __init__(self, row_data):
                            self.id = row_data[0]
                            self.pair = row_data[1]
                            self.direction = row_data[2]
                            self.ts_enter = row_data[3]
                            self.confidence = row_data[4]
                            self.timeframe = 5  # Valeur par défaut
                            self.max_gales = 2  # Valeur par défaut
                    
                    pending.append(SignalRow(row))
            
            print(f"📊 Signaux trouvés: {len(pending)}")
            
            if not pending:
                print("✅ Aucun signal en attente de vérification")
                print("="*60 + "\n")
                
                # Envoyer un message informatif aux admins
                if self.bot and self.admin_chat_ids:
                    today_stats = self._get_today_stats()
                    
                    if today_stats and today_stats['total_signals'] > 0:
                        msg = "📊 **RAPPORT DE VÉRIFICATION**\n"
                        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
                        msg += "✅ Aucun signal à vérifier maintenant\n\n"
                        msg += f"📅 **Statistiques du jour:**\n"
                        msg += f"• Total signaux: {today_stats['total_signals']}\n"
                        msg += f"• ✅ Réussis: {today_stats['wins']}\n"
                        msg += f"• ❌ Échoués: {today_stats['losses']}\n"
                        msg += f"• ⏳ En attente: {today_stats['pending']}\n"
                        
                        if today_stats['wins'] + today_stats['losses'] > 0:
                            msg += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"
                        
                        msg += "\n━━━━━━━━━━━━━━━━━━━━"
                    else:
                        msg = "📊 **RAPPORT DE VÉRIFICATION**\n"
                        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
                        msg += "✅ Aucun signal à vérifier\n\n"
                        msg += "ℹ️ Aucun signal n'a été envoyé aujourd'hui\n"
                        msg += "\n━━━━━━━━━━━━━━━━━━━━"
                    
                    for chat_id in self.admin_chat_ids:
                        try:
                            await self.bot.send_message(chat_id=chat_id, text=msg)
                            print(f"✅ Rapport envoyé à {chat_id}")
                        except Exception as e:
                            print(f"⚠️  Erreur envoi à {chat_id}: {e}")
                            import traceback
                            traceback.print_exc()
                return
            
            print(f"📊 {len(pending)} signaux à vérifier")
            print("-"*60)
            
            results = []
            verified_count = 0
            skipped_count = 0
            error_count = 0
            
            for signal in pending:
                try:
                    # Vérifier si toutes les tentatives sont terminées
                    if not self._is_signal_complete(signal):
                        skipped_count += 1
                        continue
                    
                    print(f"\n🔎 Signal #{signal.id} - {signal.pair} {signal.direction} M{signal.timeframe}")
                    result, details = await self._verify_signal_with_gales(signal)
                    
                    if result:
                        self._update_signal_result(signal.id, result, details)
                        verified_count += 1
                        results.append({
                            'signal': signal,
                            'result': result,
                            'details': details
                        })
                        
                        # Log détaillé
                        emoji = "✅" if result == 'WIN' else "❌"
                        print(f"{emoji} Résultat: {result}")
                        if details.get('winning_attempt'):
                            print(f"   Gagné à: {details['winning_attempt']}")
                        print(f"   Entrée: {details['entry_price']:.5f}")
                        print(f"   Sortie: {details['exit_price']:.5f}")
                        print(f"   Diff: {details['pips']:.1f} pips")
                    else:
                        error_count += 1
                        print(f"⚠️  Impossible de vérifier le signal #{signal.id}")
                    
                    await asyncio.sleep(2)  # Respecter limite API
                    
                except Exception as e:
                    error_count += 1
                    print(f"❌ Erreur vérification signal {signal.id}: {e}")
                    import traceback
                    traceback.print_exc()
            
            print("\n" + "-"*60)
            print(f"📈 RÉSUMÉ: {verified_count} vérifiés, {skipped_count} en attente, {error_count} erreurs")
            print("="*60 + "\n")
            
            # TOUJOURS envoyer un rapport aux admins
            if self.bot and self.admin_chat_ids:
                print(f"📤 Envoi du rapport à {len(self.admin_chat_ids)} admin(s)")
                await self._send_verification_report(results, skipped_count, error_count)
            else:
                print(f"⚠️  Impossible d'envoyer le rapport:")
                print(f"   Bot configuré: {self.bot is not None}")
                print(f"   Nombre d'admins: {len(self.admin_chat_ids)}")
            
            # Vérifier si réentraînement nécessaire
            if verified_count > 0:
                self._check_ml_retraining()
        
        except Exception as e:
            print(f"❌ ERREUR GLOBALE dans verify_pending_signals: {e}")
            import traceback
            traceback.print_exc()
            
            # Envoyer message d'erreur aux admins
            if self.bot and self.admin_chat_ids:
                error_msg = f"❌ **Erreur lors de la vérification**\n\n{str(e)}"
                for chat_id in self.admin_chat_ids:
                    try:
                        await self.bot.send_message(chat_id=chat_id, text=error_msg)
                    except:
                        pass
    
    def _is_signal_complete(self, signal):
        """Vérifie si toutes les tentatives du signal sont terminées"""
        try:
            entry_time = datetime.fromisoformat(signal.ts_enter.replace('Z', '+00:00'))
        except:
            # Si le format ISO échoue, essayer sans timezone
            entry_time = datetime.fromisoformat(signal.ts_enter)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
        
        timeframe = signal.timeframe  # en minutes (par défaut 5)
        max_attempts = signal.max_gales + 1  # signal initial + gales (par défaut 3 = 1+2)
        
        # Temps total nécessaire = timeframe * nombre de tentatives
        total_time_needed = timeframe * max_attempts
        last_attempt_end = entry_time + timedelta(minutes=total_time_needed)
        
        # Vérifier si le temps est écoulé
        now = datetime.now(timezone.utc)
        is_complete = now >= last_attempt_end
        
        if not is_complete:
            time_remaining = (last_attempt_end - now).total_seconds() / 60
            print(f"⏳ Signal #{signal.id} pas encore terminé (reste {time_remaining:.1f} min)")
        
        return is_complete
    
    async def _verify_signal_with_gales(self, signal):
        """
        Vérifie un signal en testant chaque tentative (signal + gales)
        Retourne: (result, details)
        """
        try:
            entry_time = datetime.fromisoformat(signal.ts_enter.replace('Z', '+00:00'))
        except:
            entry_time = datetime.fromisoformat(signal.ts_enter)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
        
        timeframe = signal.timeframe
        max_attempts = signal.max_gales + 1
        
        # Variables pour stocker les derniers prix (au cas où on ne trouve rien)
        last_entry_price = None
        last_exit_price = None
        last_pips_diff = 0
        
        # Tester chaque tentative
        for attempt in range(max_attempts):
            attempt_entry = entry_time + timedelta(minutes=timeframe * attempt)
            attempt_exit = attempt_entry + timedelta(minutes=timeframe)
            
            print(f"   Tentative {attempt + 1}/{max_attempts}: {attempt_entry.strftime('%H:%M:%S')}")
            
            # Récupérer les prix
            entry_price = await self._get_price_at_time(signal.pair, attempt_entry)
            if entry_price is None:
                print(f"   ⚠️  Prix d'entrée non disponible")
                continue
                
            await asyncio.sleep(1)
            exit_price = await self._get_price_at_time(signal.pair, attempt_exit)
            if exit_price is None:
                print(f"   ⚠️  Prix de sortie non disponible")
                continue
            
            # Sauvegarder pour la fin
            last_entry_price = entry_price
            last_exit_price = exit_price
            
            # Vérifier si cette tentative est gagnante
            is_winning = False
            if signal.direction == 'CALL':
                is_winning = exit_price > entry_price
            else:  # PUT
                is_winning = exit_price < entry_price
            
            pips_diff = abs(exit_price - entry_price) * 10000
            last_pips_diff = pips_diff
            
            if is_winning:
                # Victoire !
                attempt_name = "Signal initial" if attempt == 0 else f"Gale {attempt}"
                print(f"   ✅ WIN sur {attempt_name} (+{pips_diff:.1f} pips)")
                
                details = {
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'pips': pips_diff,
                    'winning_attempt': attempt_name,
                    'attempt_number': attempt + 1,
                    'total_attempts': max_attempts
                }
                return 'WIN', details
            else:
                print(f"   ❌ Tentative {attempt + 1} perdue ({pips_diff:.1f} pips)")
        
        # Toutes les tentatives ont échoué
        print(f"   ❌ LOSE après {max_attempts} tentatives")
        
        # Utiliser les derniers prix disponibles
        if last_entry_price is None or last_exit_price is None:
            print(f"   ⚠️  Impossible de récupérer les prix")
            return None, None
        
        details = {
            'entry_price': last_entry_price,
            'exit_price': last_exit_price,
            'pips': last_pips_diff,
            'winning_attempt': None,
            'attempt_number': max_attempts,
            'total_attempts': max_attempts
        }
        return 'LOSE', details
    
    async def _get_price_at_time(self, pair, timestamp):
        """Récupère le prix d'une paire à un moment donné"""
        try:
            # Chercher dans une fenêtre de 5 minutes autour du timestamp
            end_str = (timestamp + timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')
            start_str = (timestamp - timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:%S')
            
            params = {
                'symbol': pair,
                'interval': '1min',
                'outputsize': 10,
                'apikey': self.api_key,
                'format': 'JSON',
                'start_date': start_str,
                'end_date': end_str
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'values' in data and len(data['values']) > 0:
                # Trouver la bougie la plus proche du timestamp
                closest_candle = None
                min_diff = float('inf')
                
                for candle in data['values']:
                    candle_time = datetime.strptime(candle['datetime'], '%Y-%m-%d %H:%M:%S')
                    candle_time = candle_time.replace(tzinfo=timezone.utc)
                    diff = abs((candle_time - timestamp).total_seconds())
                    
                    if diff < min_diff:
                        min_diff = diff
                        closest_candle = candle
                
                if closest_candle and min_diff < 180:  # Max 3 minutes de différence
                    return float(closest_candle['close'])
            
            return None
            
        except Exception as e:
            print(f"⚠️  Erreur API: {e}")
            return None
    
    def _update_signal_result(self, signal_id, result, details):
        """Met à jour le résultat d'un signal dans la DB"""
        query = text("""
            UPDATE signals 
            SET result = :result, 
                ts_result = :ts_result,
                winning_attempt = :winning_attempt
            WHERE id = :id
        """)
        
        with self.engine.begin() as conn:
            conn.execute(query, {
                'result': result,
                'ts_result': datetime.utcnow().isoformat(),
                'winning_attempt': details.get('winning_attempt'),
                'id': signal_id
            })
        
        print(f"💾 Résultat sauvegardé: Signal #{signal_id} = {result}")
    
    async def _send_verification_report(self, results, skipped_count=0, error_count=0):
        """Envoie un rapport de vérification aux admins"""
        try:
            print("📝 Génération du rapport...")
            
            # Statistiques du jour
            today_stats = self._get_today_stats()
            
            # Rapport des signaux vérifiés maintenant
            wins = sum(1 for r in results if r['result'] == 'WIN')
            losses = len(results) - wins
            
            report = "📊 **RAPPORT DE VÉRIFICATION**\n"
            report += "━━━━━━━━━━━━━━━━━━━━\n\n"
            
            # Stats du jour TOUJOURS en premier
            if today_stats and today_stats['total_signals'] > 0:
                report += f"📅 **Statistiques du jour:**\n"
                report += f"• Total signaux: {today_stats['total_signals']}\n"
                report += f"• ✅ Réussis: {today_stats['wins']}\n"
                report += f"• ❌ Échoués: {today_stats['losses']}\n"
                report += f"• ⏳ En attente: {today_stats['pending']}\n"
                if today_stats['wins'] + today_stats['losses'] > 0:
                    report += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"
                report += "\n"
            
            # Signaux vérifiés maintenant
            if len(results) > 0:
                report += f"🔍 **Vérification actuelle:**\n"
                report += f"• Signaux vérifiés: {len(results)}\n"
                report += f"• ✅ Gains: {wins}\n"
                report += f"• ❌ Pertes: {losses}\n"
                if skipped_count > 0:
                    report += f"• ⏳ Non terminés: {skipped_count}\n"
                if error_count > 0:
                    report += f"• ⚠️ Erreurs: {error_count}\n"
                report += "\n"
                
                report += "📋 **Détails des vérifications:**\n\n"
                
                for i, r in enumerate(results[:10], 1):  # Max 10 derniers
                    emoji = "✅" if r['result'] == 'WIN' else "❌"
                    sig = r['signal']
                    det = r['details']
                    
                    attempt_info = ""
                    if det.get('winning_attempt'):
                        attempt_info = f" • {det['winning_attempt']}"
                    
                    report += f"{i}. {emoji} **{sig.pair}** {sig.direction}{attempt_info}\n"
                    report += f"   📊 {det['pips']:.1f} pips | Confiance: {sig.confidence:.0%}\n"
                    
                    if i < len(results[:10]):  # Pas de saut de ligne après le dernier
                        report += "\n"
            else:
                report += "ℹ️ Aucun signal vérifié lors de cette session\n"
                if skipped_count > 0:
                    report += f"\n⏳ **{skipped_count} signal(s) en attente**\n"
                    report += "   (Le temps nécessaire n'est pas encore écoulé)\n"
                if error_count > 0:
                    report += f"\n⚠️ {error_count} erreur(s) rencontrée(s)\n"
            
            report += "\n━━━━━━━━━━━━━━━━━━━━"
            
            print(f"📤 Envoi du rapport à {len(self.admin_chat_ids)} admin(s)...")
            
            # Envoyer à tous les admins
            sent_count = 0
            failed_count = 0
            
            for chat_id in self.admin_chat_ids:
                try:
                    print(f"   → Envoi à {chat_id}...")
                    await self.bot.send_message(
                        chat_id=chat_id, 
                        text=report,
                        parse_mode='Markdown'
                    )
                    sent_count += 1
                    print(f"   ✅ Envoyé à {chat_id}")
                except Exception as e:
                    failed_count += 1
                    print(f"   ❌ Échec pour {chat_id}: {e}")
                    import traceback
                    traceback.print_exc()
            
            print(f"\n✅ Rapport envoyé à {sent_count}/{len(self.admin_chat_ids)} admin(s)")
            if failed_count > 0:
                print(f"⚠️  {failed_count} échec(s)")
                
        except Exception as e:
            print(f"❌ ERREUR dans _send_verification_report: {e}")
            import traceback
            traceback.print_exc()
    
    def _get_today_stats(self):
        """Calcule les statistiques des signaux du jour"""
        query = text("""
            SELECT 
                COUNT(*) as total_signals,
                SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'LOSE' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
            FROM signals 
            WHERE DATE(ts_enter) = DATE('now')
        """)
        
        with self.engine.connect() as conn:
            stats = conn.execute(query).fetchone()
        
        if stats and stats.total_signals > 0:
            verified = (stats.wins or 0) + (stats.losses or 0)
            winrate = (stats.wins / verified * 100) if verified > 0 else 0
            
            return {
                'total_signals': stats.total_signals,
                'wins': stats.wins or 0,
                'losses': stats.losses or 0,
                'pending': stats.pending or 0,
                'winrate': winrate
            }
        
        return None
    
    def _check_ml_retraining(self):
        """Vérifie si réentraînement ML nécessaire"""
        query = text("""
            SELECT COUNT(*) as count 
            FROM signals 
            WHERE result IS NOT NULL
        """)
        
        with self.engine.connect() as conn:
            count = conn.execute(query).scalar()
        
        if count >= 100 and count % 50 == 0:
            print(f"\n🎓 {count} résultats disponibles")
            print(f"💡 Réentraînement du modèle ML recommandé")
            print(f"   Utilisez /train pour améliorer la précision\n")
    
    def get_performance_stats(self):
        """Calcule les statistiques de performance globales"""
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'LOSE' THEN 1 ELSE 0 END) as losses,
                AVG(confidence) as avg_confidence
            FROM signals 
            WHERE result IS NOT NULL
        """)
        
        with self.engine.connect() as conn:
            stats = conn.execute(query).fetchone()
        
        if stats and stats.total > 0:
            winrate = (stats.wins / stats.total) * 100
            return {
                'total': stats.total,
                'wins': stats.wins or 0,
                'losses': stats.losses or 0,
                'winrate': winrate,
                'avg_confidence': stats.avg_confidence or 0
            }
        
        return None
    
    def get_recent_results(self, limit=10):
        """Récupère les derniers résultats vérifiés"""
        query = text("""
            SELECT pair, direction, result, confidence, 
                   COALESCE(timeframe, 5) as timeframe,
                   winning_attempt, ts_enter, ts_result
            FROM signals 
            WHERE result IS NOT NULL
            ORDER BY ts_result DESC
            LIMIT :limit
        """)
        
        with self.engine.connect() as conn:
            results = conn.execute(query, {'limit': limit}).fetchall()
        
        return results
    
    async def send_daily_summary(self):
        """Envoie un résumé quotidien aux admins"""
        stats = self._get_today_stats()
        
        if not stats or stats['total_signals'] == 0:
            return
        
        report = "📊 **RÉSUMÉ QUOTIDIEN**\n"
        report += "━━━━━━━━━━━━━━━━━━━━\n\n"
        report += f"📅 Date: {datetime.now().strftime('%d/%m/%Y')}\n\n"
        report += f"📈 **Résultats:**\n"
        report += f"• Total signaux: {stats['total_signals']}\n"
        report += f"• ✅ Réussis: {stats['wins']}\n"
        report += f"• ❌ Échoués: {stats['losses']}\n"
        report += f"• ⏳ En attente: {stats['pending']}\n\n"
        
        if stats['wins'] + stats['losses'] > 0:
            report += f"📊 **Performance:**\n"
            report += f"• Win rate: {stats['winrate']:.1f}%\n"
            
            # Ajouter évaluation
            if stats['winrate'] >= 70:
                report += f"• 🎉 Excellente performance !\n"
            elif stats['winrate'] >= 60:
                report += f"• 👍 Bonne performance\n"
            else:
                report += f"• ⚠️  Performance à améliorer\n"
        
        report += "\n━━━━━━━━━━━━━━━━━━━━"
        
        for chat_id in self.admin_chat_ids:
            try:
                await self.bot.send_message(chat_id=chat_id, text=report)
            except Exception as e:
                print(f"⚠️  Erreur envoi résumé à {chat_id}: {e}")
