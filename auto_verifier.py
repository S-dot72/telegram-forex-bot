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
        
        # Paramètres par défaut
        self.default_timeframe = 5  # minutes    
        self.default_max_gales = 2  # 2 gales (3 tentatives total)    
        self._session = requests.Session()

    def set_bot(self, bot):
        """Configure le bot pour les notifications"""
        self.bot = bot
        print("✅ Bot configuré pour les notifications")

    def add_admin(self, chat_id):
        """Ajoute un admin pour recevoir les rapports"""
        if chat_id not in self.admin_chat_ids:
            self.admin_chat_ids.append(chat_id)
            print(f"✅ Admin {chat_id} ajouté")

    async def verify_pending_signals(self):
        """Vérifie tous les signaux qui n'ont pas encore de résultat - TOUT EN UTC"""
        try:
            now_utc = datetime.now(timezone.utc)
            print("\n" + "="*60)
            print(f"🔍 VÉRIFICATION AUTOMATIQUE - {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print("="*60)

            # Récupérer les signaux sans résultat    
            query = text("""    
                SELECT id, pair, direction, ts_enter, confidence    
                FROM signals     
                WHERE result IS NULL    
                ORDER BY ts_enter DESC    
                LIMIT 50    
            """)    
                
            with self.engine.connect() as conn:    
                pending = conn.execute(query).fetchall()    
                
            print(f"📊 Signaux sans résultat: {len(pending)}")    
                
            if not pending:    
                print("✅ Aucun signal en attente")    
                print("="*60 + "\n")    
                    
                if self.bot and self.admin_chat_ids:    
                    await self._send_no_pending_report()    
                return    
                
            print(f"📊 {len(pending)} signaux à vérifier")    
            print("-"*60)    
                
            results = []    
            verified_count = 0    
            skipped_count = 0    
            error_count = 0    
                
            for signal_row in pending:    
                try:    
                    signal_id = signal_row[0]    
                    pair = signal_row[1]    
                    direction = signal_row[2]    
                    ts_enter = signal_row[3]    
                    confidence = signal_row[4] if signal_row[4] else 0.5    
                        
                    print(f"\n{'='*40}")    
                    print(f"🔎 Signal #{signal_id} - {pair} {direction}")    
                    print(f"{'='*40}")    
                        
                    # CORRECTION: Vérifier en UTC avec la fonction corrigée
                    if not self._is_signal_complete_utc(ts_enter):    
                        skipped_count += 1    
                        print(f"➡️  SKIP - Signal pas prêt\n")    
                        continue    
                        
                    print(f"✅ Signal prêt pour vérification")    
                        
                    # Vérifier le signal    
                    result, details = await self._verify_signal_with_gales(    
                        signal_id, pair, direction, ts_enter    
                    )    
                        
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
                        print(f"{emoji} Résultat: {result}")    
                        if details and details.get('gale_level') is not None:    
                            gale_text = ["Signal initial", "Gale 1", "Gale 2"][details['gale_level']]    
                            print(f"   Gagné à: {gale_text}")    
                    else:    
                        error_count += 1    
                        print(f"⚠️  Impossible de vérifier #{signal_id}")    
                    
                    await asyncio.sleep(1.5)    
                        
                except Exception as e:    
                    error_count += 1    
                    print(f"❌ Erreur: {e}")    
                    import traceback    
                    traceback.print_exc()    
            
            print("\n" + "-"*60)    
            print(f"📈 RÉSUMÉ: {verified_count} vérifiés, {skipped_count} en attente, {error_count} erreurs")    
            print("="*60 + "\n")    
                
            if self.bot and self.admin_chat_ids:    
                print(f"📤 Envoi rapport à {len(self.admin_chat_ids)} admin(s)")    
                await self._send_verification_report(results, skipped_count, error_count)    
        
        except Exception as e:    
            print(f"❌ ERREUR GLOBALE: {e}")    
            import traceback    
            traceback.print_exc()    
                
            if self.bot and self.admin_chat_ids:    
                error_msg = f"❌ **Erreur vérification**\n\n{str(e)[:200]}"    
                for chat_id in self.admin_chat_ids:    
                    try:    
                        await self.bot.send_message(chat_id=chat_id, text=error_msg)    
                    except:    
                        pass

    def _is_signal_complete_utc(self, ts_enter):
        """Vérifie si signal complet - VERSION CORRIGÉE"""
        try:
            # CORRECTION: Parser timestamp de manière robuste
            if isinstance(ts_enter, str):
                # Nettoyer le timestamp
                ts_clean = ts_enter.replace('Z', '').replace('+00:00', '').split('.')[0]
                try:
                    entry_time_utc = datetime.fromisoformat(ts_clean)
                except:
                    # Essayer un autre format
                    entry_time_utc = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
            else:
                entry_time_utc = ts_enter
            
            # S'assurer que c'est en UTC
            if entry_time_utc.tzinfo is None:
                entry_time_utc = entry_time_utc.replace(tzinfo=timezone.utc)
            else:
                entry_time_utc = entry_time_utc.astimezone(timezone.utc)

            # Calculer fin en UTC (15 minutes après l'entrée)
            end_time_utc = entry_time_utc + timedelta(minutes=15)
            
            # Maintenant en UTC
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

    async def _verify_signal_with_gales(self, signal_id, pair, direction, ts_enter):
        """Vérifie signal avec gales - TOUT EN UTC, PAS DE CONVERSION"""
        try:
            # CORRECTION: Parser timestamp de manière robuste
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

            max_attempts = 3  # signal initial + 2 gales    
                
            last_entry_price = None    
            last_exit_price = None    
            last_pips_diff = 0    
                
            for attempt in range(max_attempts):    
                # Calcul des timestamps en UTC    
                attempt_entry_utc = entry_time_utc + timedelta(minutes=5 * attempt)    
                attempt_exit_utc = attempt_entry_utc + timedelta(minutes=5)    
                    
                print(f"   Tentative {attempt + 1}/3: {attempt_entry_utc.strftime('%H:%M')} UTC")    
                    
                # Récupérer prix    
                entry_price = await self._get_price_at_time(pair, attempt_entry_utc)    
                if entry_price is None:    
                    print(f"   ⚠️  Prix d'entrée non disponible")    
                    continue    
                    
                await asyncio.sleep(0.5)    
                    
                exit_price = await self._get_price_at_time(pair, attempt_exit_utc)    
                if exit_price is None:    
                    print(f"   ⚠️  Prix de sortie non disponible")    
                    last_entry_price = entry_price    
                    continue    
                    
                last_entry_price = entry_price    
                last_exit_price = exit_price    
                    
                # Déterminer WIN/LOSE    
                is_winning = (exit_price > entry_price) if direction == 'CALL' else (exit_price < entry_price)    
                    
                pips_diff = abs(exit_price - entry_price) * 10000    
                last_pips_diff = pips_diff    
                    
                if is_winning:    
                    print(f"   ✅ WIN tentative {attempt + 1} (+{pips_diff:.1f} pips)")    
                    details = {    
                        'entry_price': entry_price,    
                        'exit_price': exit_price,    
                        'pips': pips_diff,    
                        'gale_level': attempt    
                    }    
                    return 'WIN', details    
                else:    
                    print(f"   ❌ Tentative {attempt + 1} perdue ({pips_diff:.1f} pips)")    
            
            # Toutes tentatives perdues    
            print(f"   ❌ LOSE après {max_attempts} tentatives")    
                
            if last_entry_price is None or last_exit_price is None:    
                print(f"   ⚠️  Pas assez de prix")    
                return None, None    
                
            details = {    
                'entry_price': last_entry_price,    
                'exit_price': last_exit_price,    
                'pips': last_pips_diff,    
                'gale_level': None    
            }    
            return 'LOSE', details    
                
        except Exception as e:    
            print(f"❌ Erreur: {e}")    
            import traceback    
            traceback.print_exc()    
            return None, None

    async def _get_price_at_time(self, pair, timestamp):
        """Récupère prix à un moment donné (timestamp en UTC) - VERSION AMÉLIORÉE"""
        try:
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            ts_utc = timestamp.astimezone(timezone.utc)    
            
            # CORRECTION: Utiliser une plage plus large pour être sûr de trouver une bougie
            start_dt = ts_utc - timedelta(minutes=10)    
            end_dt = ts_utc + timedelta(minutes=10)    
                
            start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')    
            end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')    
                
            params = {    
                'symbol': pair,    
                'interval': '1min',    
                'outputsize': 20,    
                'apikey': self.api_key,    
                'format': 'JSON',    
                'start_date': start_str,    
                'end_date': end_str    
            }    
                
            print(f"   🔍 Requête API: {pair} autour de {ts_utc.strftime('%H:%M:%S')} UTC")
                
            resp = self._session.get(self.base_url, params=params, timeout=12)    
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
                
                if closest_candle and min_diff <= 300:  # 5 minutes de tolérance    
                    try:    
                        price = float(closest_candle['close'])    
                        print(f"   💰 Prix trouvé: {price} (diff: {min_diff:.0f}s)")    
                        return price    
                    except:    
                        return None    
            
            print(f"   ⚠️  Aucune bougie trouvée pour {pair}")    
            return None    
                
        except Exception as e:    
            print(f"⚠️  Erreur API pour {pair}: {e}")    
            return None

    def _update_signal_result(self, signal_id, result, details):
        """Met à jour résultat dans DB"""
        try:
            gale_level = 0
            if details and isinstance(details, dict) and details.get('gale_level') is not None:
                gale_level = details.get('gale_level', 0)

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
                
            print(f"💾 Résultat sauvegardé: #{signal_id} = {result}")    
                
        except Exception as e:    
            print(f"❌ Erreur _update_signal_result: {e}")    
            try:    
                query = text("UPDATE signals SET result = :result WHERE id = :id")    
                with self.engine.begin() as conn:    
                    conn.execute(query, {'result': result, 'id': signal_id})    
                print(f"💾 Sauvegardé (version simple)")    
            except Exception as e2:    
                print(f"❌ Échec total: {e2}")

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

    async def _send_verification_report(self, results, skipped_count=0, error_count=0):
        """Envoie rapport de vérification"""
        try:
            print("📝 Génération rapport...")

            today_stats = self._get_today_stats()    
            wins = sum(1 for r in results if r.get('result') == 'WIN')    
            losses = len(results) - wins    
                
            report = "📊 **RAPPORT DE VÉRIFICATION**\n"    
            report += "━━━━━━━━━━━━━━━━━━━━\n\n"    
                
            if today_stats and today_stats['total_signals'] > 0:    
                report += f"📅 **Stats du jour:**\n"    
                report += f"• Total: {today_stats['total_signals']}\n"    
                report += f"• ✅ Réussis: {today_stats['wins']}\n"    
                report += f"• ❌ Échoués: {today_stats['losses']}\n"    
                report += f"• ⏳ En attente: {today_stats['pending']}\n"    
                if today_stats['wins'] + today_stats['losses'] > 0:    
                    report += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"    
                report += "\n"    
                
            if len(results) > 0:    
                report += f"🔍 **Vérification actuelle:**\n"    
                report += f"• Vérifiés: {len(results)}\n"    
                report += f"• ✅ Gains: {wins}\n"    
                report += f"• ❌ Pertes: {losses}\n"    
                if skipped_count > 0:    
                    report += f"• ⏳ Non terminés: {skipped_count}\n"    
                if error_count > 0:    
                    report += f"• ⚠️ Erreurs: {error_count}\n"    
                report += "\n📋 **Détails:**\n\n"    
                    
                for i, r in enumerate(results[:10], 1):    
                    emoji = "✅" if r['result'] == 'WIN' else "❌"    
                    gale_level = r['details'].get('gale_level') if r.get('details') else None    
                        
                    gale_text = ""    
                    if r['result'] == 'WIN' and gale_level is not None:    
                        gale_names = ["Signal initial", "Gale 1", "Gale 2"]    
                        if gale_level < len(gale_names):    
                            gale_text = f" • {gale_names[gale_level]}"    
                    
                    report += f"{i}. {emoji} **{r['pair']}** {r['direction']}{gale_text}\n"    
                    report += f"   📊 {r['details'].get('pips', 0):.1f} pips\n\n"    
            else:    
                report += "ℹ️ Aucun signal vérifié\n"    
                if skipped_count > 0:    
                    report += f"\n⏳ {skipped_count} signal(s) en attente\n"    
                
            report += "\n━━━━━━━━━━━━━━━━━━━━"    
                
            print(f"📤 Envoi à {len(self.admin_chat_ids)} admin(s)")    
                
            sent_count = 0    
            for chat_id in self.admin_chat_ids:    
                try:    
                    await self.bot.send_message(chat_id=chat_id, text=report)    
                    sent_count += 1    
                    print(f"   ✅ Envoyé à {chat_id}")    
                except Exception as e:    
                    print(f"   ❌ Échec {chat_id}: {e}")    
            
            print(f"✅ Rapport envoyé à {sent_count}/{len(self.admin_chat_ids)}")    
                    
        except Exception as e:    
            print(f"❌ Erreur rapport: {e}")    
            import traceback    
            traceback.print_exc()

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
