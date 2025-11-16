import asyncio
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
import requests
import time
from collections import deque

class AutoResultVerifier:
    def __init__(self, engine, twelvedata_api_key, bot=None):
        self.engine = engine
        self.api_key = twelvedata_api_key
        self.base_url = 'https://api.twelvedata.com/time_series'
        self.bot = bot
        self.admin_chat_ids = []
        
        # Gestion stricte des limites API
        self.api_call_times = deque()
        self.max_per_minute = 6  # Marge de sécurité
        self._session = requests.Session()
        
        print("🤖 Vérificateur DONNÉES RÉELLES initialisé")

    def can_make_api_call(self):
        """Vérifie si on peut faire un appel API sans dépasser la limite"""
        now = time.time()
        
        # Nettoyer les appels vieux de plus d'1 minute
        while self.api_call_times and now - self.api_call_times[0] > 60:
            self.api_call_times.popleft()
        
        # Vérifier la limite
        if len(self.api_call_times) >= self.max_per_minute:
            time_to_wait = 60 - (now - self.api_call_times[0])
            print(f"⏸️  Limite API: {len(self.api_call_times)}/{self.max_per_minute} - Attente: {time_to_wait:.1f}s")
            return False, time_to_wait
        
        return True, 0

    async def safe_api_call(self, pair, timestamp):
        """Appel API sécurisé avec respect strict des limites"""
        can_call, wait_time = self.can_make_api_call()
        
        if not can_call:
            print(f"⏳ Attente de {wait_time:.1f} secondes pour respecter les limites API...")
            await asyncio.sleep(wait_time + 1)
        
        # Faire l'appel
        self.api_call_times.append(time.time())
        return await self._get_real_price_at_time(pair, timestamp)

    async def verify_pending_signals_real_data(self):
        """Vérification UNIQUEMENT avec données réelles"""
        try:
            now_utc = datetime.now(timezone.utc)
            print("\n" + "="*60)
            print(f"🔍 VÉRIFICATION DONNÉES RÉELLES - {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"📊 Statut API: {len(self.api_call_times)}/{self.max_per_minute} appels cette minute")
            print("🚨 ATTENTION: Pas de fallback simulé - Données réelles uniquement")
            print("="*60)

            # Vérifier combien de signaux sont prêts
            ready_signals = []
            with self.engine.connect() as conn:
                pending = conn.execute(text("""
                    SELECT id, pair, direction, ts_enter, confidence    
                    FROM signals     
                    WHERE result IS NULL    
                    ORDER BY ts_enter DESC    
                    LIMIT 10
                """)).fetchall()
                
                for signal in pending:
                    signal_id, pair, direction, ts_enter, confidence = signal
                    if self._is_signal_complete_utc(ts_enter):
                        ready_signals.append(signal)
                    else:
                        print(f"⏳ Signal #{signal_id} pas encore prêt")
            
            print(f"📊 Signaux prêts à vérifier: {len(ready_signals)}/{len(pending)}")
            
            if not ready_signals:
                print("✅ Aucun signal prêt pour vérification")
                if self.bot and self.admin_chat_ids:
                    await self._send_no_pending_report()
                return
            
            # Limiter strictement pour éviter les dépassements
            signals_to_check = ready_signals[:2]  # Max 2 signaux par cycle
            print(f"🔍 Vérification de {len(signals_to_check)} signaux (limite stricte)")
            
            results = []
            verified_count = 0
            error_count = 0
            api_limited_count = 0
            
            for signal_row in signals_to_check:
                try:
                    signal_id, pair, direction, ts_enter, confidence = signal_row
                    
                    print(f"\n{'='*40}")
                    print(f"🔎 Signal #{signal_id} - {pair} {direction}")
                    print(f"{'='*40}")
                    
                    # Vérification UNIQUEMENT avec données réelles
                    result, details = await self._verify_with_real_data_only(signal_id, pair, direction, ts_enter)
                    
                    if result:
                        self._update_signal_result(signal_id, result, details)
                        verified_count += 1
                        results.append({
                            'signal_id': signal_id,
                            'pair': pair,
                            'direction': direction,
                            'result': result,
                            'details': details or {},
                            'confidence': confidence
                        })
                        
                        emoji = "✅" if result == 'WIN' else "❌"
                        print(f"{emoji} Résultat RÉEL: {result}")
                        if details and details.get('gale_level') is not None:
                            gale_text = ["Signal initial", "Gale 1", "Gale 2"][details['gale_level']]
                            print(f"   Gagné à: {gale_text}")
                    
                    elif result is None:
                        # API limitée - on laisse le signal en attente
                        api_limited_count += 1
                        print(f"🔄 Signal #{signal_id} laissé en attente (limite API)")
                    
                    else:
                        error_count += 1
                        print(f"⚠️  Impossible de vérifier #{signal_id} avec données réelles")
                    
                    # Attente stratégique entre les signaux
                    if len(signals_to_check) > 1:
                        wait_time = 30
                        print(f"⏳ Attente de {wait_time}s entre les signaux...")
                        await asyncio.sleep(wait_time)
                        
                except Exception as e:
                    error_count += 1
                    print(f"❌ Erreur: {e}")
                    import traceback
                    traceback.print_exc()
            
            print("\n" + "-"*60)
            print(f"📈 RÉSUMÉ RÉEL: {verified_count} vérifiés, {api_limited_count} en attente (API), {error_count} erreurs")
            print(f"📊 Utilisation API: {len(self.api_call_times)} appels cette minute")
            
            if api_limited_count > 0:
                print("💡 Conseil: Certains signaux sont en attente à cause des limites API")
                print("💡 Ils seront vérifiés automatiquement au prochain cycle")
            
            print("="*60 + "\n")
            
            if self.bot and self.admin_chat_ids:
                print(f"📤 Envoi rapport à {len(self.admin_chat_ids)} admin(s)")
                await self._send_real_data_report(results, api_limited_count, error_count)
        
        except Exception as e:
            print(f"❌ ERREUR GLOBALE: {e}")
            import traceback
            traceback.print_exc()

    async def _verify_with_real_data_only(self, signal_id, pair, direction, ts_enter):
        """Vérification UNIQUEMENT avec données API réelles"""
        try:
            # Parser timestamp
            if isinstance(ts_enter, str):
                ts_clean = ts_enter.replace('Z', '').replace('+00:00', '').split('.')[0]
                try:
                    entry_time_utc = datetime.fromisoformat(ts_clean)
                except:
                    entry_time_utc = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
            else:
                entry_time_utc = ts_enter
            
            if entry_time_utc.tzinfo is None:
                entry_time_utc = entry_time_utc.replace(tzinfo=timezone.utc)

            max_attempts = 3
            prices_found = 0
            
            for attempt in range(max_attempts):
                attempt_entry_utc = entry_time_utc + timedelta(minutes=5 * attempt)
                attempt_exit_utc = attempt_entry_utc + timedelta(minutes=5)
                    
                print(f"   Tentative {attempt + 1}/3: {attempt_entry_utc.strftime('%H:%M')} UTC")
                    
                # Appel API sécurisé - DONNÉES RÉELLES UNIQUEMENT
                entry_price = await self.safe_api_call(pair, attempt_entry_utc)
                if entry_price is None:
                    print(f"   ⚠️  Prix d'entrée non disponible (limite API?)")
                    # Si API limitée, on arrête et on laisse le signal en attente
                    if len(self.api_call_times) >= self.max_per_minute:
                        print("   🚨 LIMITE API - Arrêt de la vérification de ce signal")
                        return None, None  # Signal reste en attente
                    continue
                    
                # Petit délai entre entrée et sortie
                await asyncio.sleep(2)
                    
                exit_price = await self.safe_api_call(pair, attempt_exit_utc)
                if exit_price is None:
                    print(f"   ⚠️  Prix de sortie non disponible (limite API?)")
                    if len(self.api_call_times) >= self.max_per_minute:
                        print("   🚨 LIMITE API - Arrêt de la vérification de ce signal")
                        return None, None  # Signal reste en attente
                    continue
                    
                prices_found += 1
                    
                # Déterminer WIN/LOSE avec données RÉELLES
                is_winning = (exit_price > entry_price) if direction == 'CALL' else (exit_price < entry_price)
                pips_diff = abs(exit_price - entry_price) * 10000

                if is_winning:
                    print(f"   ✅ WIN RÉEL tentative {attempt + 1} (+{pips_diff:.1f} pips)")
                    details = {
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'pips': pips_diff,
                        'gale_level': attempt,
                        'source': 'API_RÉELLE'
                    }
                    return 'WIN', details
                else:
                    print(f"   ❌ PERTE RÉELLE tentative {attempt + 1} ({pips_diff:.1f} pips)")
            
            if prices_found > 0:
                print(f"   ❌ LOSE RÉEL après {max_attempts} tentatives")
                return 'LOSE', {'gale_level': None, 'source': 'API_RÉELLE'}
            else:
                print("   ⚠️  Aucun prix RÉEL trouvé - Signal reste en attente")
                return None, None  # Signal reste en attente pour prochaine vérification
                
        except Exception as e:
            print(f"❌ Erreur vérification données réelles: {e}")
            return None, None

    async def _get_real_price_at_time(self, pair, timestamp):
        """Récupère le prix RÉEL à un moment donné - PAS DE SIMULATION"""
        try:
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            ts_utc = timestamp.astimezone(timezone.utc)
            
            # Intervalle réduit pour économiser les appels
            start_dt = ts_utc - timedelta(minutes=5)
            end_dt = ts_utc + timedelta(minutes=5)
                
            start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
                
            params = {
                'symbol': pair,
                'interval': '1min',
                'outputsize': 10,
                'apikey': self.api_key,
                'format': 'JSON',
                'start_date': start_str,
                'end_date': end_str
            }
                
            print(f"   🔍 Requête API RÉELLE: {pair} autour de {ts_utc.strftime('%H:%M:%S')} UTC")
                
            resp = self._session.get(self.base_url, params=params, timeout=10)
            
            if resp.status_code == 429:
                print("   🚨 LIMITE API ATTEINTE - Code 429")
                return None  # Pas de fallback!
                
            resp.raise_for_status()
            data = resp.json()
                
            if 'values' in data and len(data['values']) > 0:
                closest_candle = None
                min_diff = float('inf')
                    
                for candle in data['values']:
                    try:
                        candle_time = datetime.fromisoformat(candle['datetime'].replace('Z', '+00:00'))
                    except:
                        try:
                            candle_time = datetime.strptime(candle['datetime'], '%Y-%m-%d %H:%M:%S')
                        except:
                            continue
                        
                    if candle_time.tzinfo is None:
                        candle_time = candle_time.replace(tzinfo=timezone.utc)
                        
                    diff = abs((candle_time - ts_utc).total_seconds())
                    if diff < min_diff:
                        min_diff = diff
                        closest_candle = candle
                
                if closest_candle and min_diff <= 300:
                    try:
                        price = float(closest_candle['close'])
                        print(f"   💰 Prix RÉEL trouvé: {price} (diff: {min_diff:.0f}s)")
                        return price
                    except:
                        return None
            
            print(f"   ⚠️  Aucune bougie RÉELLE trouvée pour {pair}")
            return None  # Pas de fallback!
                
        except Exception as e:
            print(f"⚠️  Erreur API pour {pair}: {e}")
            return None  # Pas de fallback!

    def _is_signal_complete_utc(self, ts_enter):
        """Vérifie si signal complet - Version corrigée"""
        try:
            if isinstance(ts_enter, str):
                ts_clean = ts_enter.replace('Z', '').replace('+00:00', '').split('.')[0]
                try:
                    entry_time_utc = datetime.fromisoformat(ts_clean)
                except:
                    entry_time_utc = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
            else:
                entry_time_utc = ts_enter
            
            if entry_time_utc.tzinfo is None:
                entry_time_utc = entry_time_utc.replace(tzinfo=timezone.utc)
            else:
                entry_time_utc = entry_time_utc.astimezone(timezone.utc)

            end_time_utc = entry_time_utc + timedelta(minutes=15)
            now_utc = datetime.now(timezone.utc)
            
            is_complete = now_utc >= end_time_utc
            
            print(f"   📅 Entrée UTC: {entry_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📅 Fin UTC: {end_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📅 Maintenant UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   ⏱️  Temps restant: {(end_time_utc - now_utc).total_seconds()/60:.1f} min")
            print(f"   {'✅ COMPLET' if is_complete else '⏳ PAS COMPLET'}")
            
            return is_complete
            
        except Exception as e:
            print(f"❌ Erreur _is_signal_complete_utc: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _update_signal_result(self, signal_id, result, details):
        """Met à jour résultat dans DB"""
        try:
            gale_level = details.get('gale_level', 0) if details else 0

            query = text("""    
                UPDATE signals     
                SET result = :result, gale_level = :gale_level    
                WHERE id = :id    
            """)
                
            with self.engine.begin() as conn:
                conn.execute(query, {
                    'result': result,
                    'gale_level': gale_level,
                    'id': signal_id
                })
                
            print(f"💾 Résultat RÉEL sauvegardé: #{signal_id} = {result}")
                
        except Exception as e:
            print(f"❌ Erreur sauvegarde: {e}")
            try:
                query = text("UPDATE signals SET result = :result WHERE id = :id")
                with self.engine.begin() as conn:
                    conn.execute(query, {'result': result, 'id': signal_id})
                print(f"💾 Sauvegardé (version simple)")
            except Exception as e2:
                print(f"❌ Échec total sauvegarde: {e2}")

    async def _send_real_data_report(self, results, api_limited_count, error_count):
        """Rapport spécial pour données réelles uniquement"""
        try:
            print("📝 Génération rapport DONNÉES RÉELLES...")

            today_stats = self._get_today_stats()    
            wins = sum(1 for r in results if r.get('result') == 'WIN')    
            losses = len(results) - wins    
                
            report = "📊 **RAPPORT DONNÉES RÉELLES**\n"    
            report += "🚨 *Données réelles uniquement - Pas de simulation*\n"  
            report += "━━━━━━━━━━━━━━━━━━━━\n\n"    
                
            if today_stats and today_stats['total_signals'] > 0:    
                report += f"📅 **Stats du jour (RÉELLES):**\n"    
                report += f"• Total: {today_stats['total_signals']}\n"    
                report += f"• ✅ Réussis: {today_stats['wins']}\n"    
                report += f"• ❌ Échoués: {today_stats['losses']}\n"    
                report += f"• ⏳ En attente: {today_stats['pending']}\n"    
                if today_stats['wins'] + today_stats['losses'] > 0:    
                    report += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"    
                report += "\n"    
                
            if len(results) > 0:    
                report += f"🔍 **Vérification actuelle (RÉELLE):**\n"    
                report += f"• Vérifiés: {len(results)}\n"    
                report += f"• ✅ Gains: {wins}\n"    
                report += f"• ❌ Pertes: {losses}\n"    
                if api_limited_count > 0:    
                    report += f"• 🔄 En attente (limite API): {api_limited_count}\n"    
                if error_count > 0:    
                    report += f"• ⚠️ Erreurs: {error_count}\n"    
                report += "\n📋 **Détails (RÉELS):**\n\n"    
                    
                for i, r in enumerate(results[:10], 1):    
                    emoji = "✅" if r['result'] == 'WIN' else "❌"    
                    gale_level = r['details'].get('gale_level') if r.get('details') else None    
                        
                    gale_text = ""    
                    if r['result'] == 'WIN' and gale_level is not None:    
                        gale_names = ["Signal initial", "Gale 1", "Gale 2"]    
                        if gale_level < len(gale_names):    
                            gale_text = f" • {gale_names[gale_level]}"    
                    
                    report += f"{i}. {emoji} **{r['pair']}** {r['direction']}{gale_text} 🔗\n"    
                    report += f"   📊 {r['details'].get('pips', 0):.1f} pips RÉELS\n\n"    
            else:    
                report += "ℹ️ Aucun signal vérifié avec données réelles\n"    
                if api_limited_count > 0:    
                    report += f"\n🔄 {api_limited_count} signal(s) en attente (limite API)\n"  
                
            report += "\n━━━━━━━━━━━━━━━━━━━━"    
            report += "\n🚨 *Tous les résultats proviennent de données marché réelles*"    
                
            print(f"📤 Envoi à {len(self.admin_chat_ids)} admin(s)")    
                
            sent_count = 0    
            for chat_id in self.admin_chat_ids:    
                try:    
                    await self.bot.send_message(chat_id=chat_id, text=report)    
                    sent_count += 1    
                    print(f"   ✅ Envoyé à {chat_id}")    
                except Exception as e:    
                    print(f"   ❌ Échec {chat_id}: {e}")    
            
            print(f"✅ Rapport DONNÉES RÉELLES envoyé à {sent_count}/{len(self.admin_chat_ids)}")    
                    
        except Exception as e:    
            print(f"❌ Erreur rapport: {e}")

    async def _send_no_pending_report(self):
        """Rapport quand rien à vérifier"""
        today_stats = self._get_today_stats()

        msg = "📊 **RAPPORT DE VÉRIFICATION**\n"    
        msg += "━━━━━━━━━━━━━━━━━━━━\n\n"    
        msg += "✅ Aucun signal à vérifier\n\n"    
            
        if today_stats and today_stats['total_signals'] > 0:    
            msg += f"📅 **Stats du jour:**\n"    
            msg += f"• Total: {today_stats['total_signals']}\n"    
            msg += f"• ✅ Réussis: {today_stats['wins']}\n"    
            msg += f"• ❌ Échoués: {today_stats['losses']}\n"    
            msg += f"• ⏳ En attente: {today_stats['pending']}\n"    
            if today_stats['wins'] + today_stats['losses'] > 0:    
                msg += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"    
            
        msg += "\n━━━━━━━━━━━━━━━━━━━━"    
            
        for chat_id in self.admin_chat_ids:    
            try:    
                await self.bot.send_message(chat_id=chat_id, text=msg)    
            except Exception as e:    
                print(f"❌ Envoi à {chat_id}: {e}")

    def _get_today_stats(self):
        """Stats du jour"""
        try:
            now_utc = datetime.now(timezone.utc)
            start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            end_utc = start_utc + timedelta(days=1)

            query = text("""    
                SELECT     
                    COUNT(*) as total,    
                    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,    
                    SUM(CASE WHEN result = 'LOSE' THEN 1 ELSE 0 END) as losses,    
                    SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending    
                FROM signals     
                WHERE ts_enter >= :start AND ts_enter < :end    
            """)    
                
            with self.engine.connect() as conn:    
                stats = conn.execute(query, {    
                    "start": start_utc.isoformat(),    
                    "end": end_utc.isoformat()    
                }).fetchone()    
                
            if stats and stats[0] > 0:    
                total = stats[0]    
                wins = stats[1] or 0    
                losses = stats[2] or 0    
                pending = stats[3] or 0    
                    
                verified = wins + losses    
                winrate = (wins / verified * 100) if verified > 0 else 0    
                    
                return {    
                    'total_signals': total,    
                    'wins': wins,    
                    'losses': losses,    
                    'pending': pending,    
                    'winrate': winrate    
                }    
                
            return None    
                
        except Exception as e:    
            print(f"❌ Erreur stats: {e}")    
            return None

    def set_bot(self, bot):
        """Configure le bot pour les notifications"""
        self.bot = bot
        print("✅ Bot configuré pour les notifications")

    def add_admin(self, chat_id):
        """Ajoute un admin pour recevoir les rapports"""
        if chat_id not in self.admin_chat_ids:
            self.admin_chat_ids.append(chat_id)
            print(f"✅ Admin {chat_id} ajouté")
