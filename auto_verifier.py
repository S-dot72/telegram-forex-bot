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
        
        # Paramètres pour M1 SANS GALE
        self.default_timeframe = 1  # 1 minute (M1)
        self.default_max_gales = 0  # SANS GALE
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

    def _is_weekend(self, timestamp):
        """Vérifie si le timestamp tombe le week-end (marché fermé)"""
        if isinstance(timestamp, str):
            ts_clean = timestamp.replace('Z', '').replace('+00:00', '').split('.')[0]
            try:
                dt = datetime.fromisoformat(ts_clean)
            except:
                try:
                    dt = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
                except:
                    return True
        else:
            dt = timestamp
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        weekday = dt.weekday()
        hour = dt.hour
        
        # Samedi : toujours fermé
        if weekday == 5:
            return True
        
        # Dimanche : fermé avant 22h UTC
        if weekday == 6 and hour < 22:
            return True
        
        # Vendredi : fermé après 22h UTC
        if weekday == 4 and hour >= 22:
            return True
        
        return False

    async def verify_single_signal(self, signal_id):
        """
        Vérifie UN SEUL signal en M1 SANS GALE
        Vérification immédiate 1 minute après l'entrée
        """
        try:
            with self.engine.connect() as conn:
                signal = conn.execute(
                    text("""
                        SELECT id, pair, direction, ts_enter, confidence
                        FROM signals
                        WHERE id = :sid AND result IS NULL
                    """),
                    {"sid": signal_id}
                ).fetchone()
            
            if not signal:
                print(f"⚠️ Signal #{signal_id} déjà vérifié ou inexistant")
                return None
            
            signal_id, pair, direction, ts_enter, confidence = signal
            
            print(f"\n🔍 Vérification M1 signal #{signal_id} - {pair} {direction}")
            
            # Vérifier si week-end
            if self._is_weekend(ts_enter):
                print(f"🏖️ Signal du week-end - Marqué comme LOSE")
                self._update_signal_result(signal_id, 'LOSE', {
                    'entry_price': 0,
                    'exit_price': 0,
                    'pips': 0,
                    'gale_level': 0,
                    'reason': 'Marché fermé (week-end)'
                })
                return 'LOSE'
            
            # Vérifier si le signal M1 est complet
            if not self._is_signal_complete_m1(ts_enter):
                print(f"⏳ Signal M1 pas encore prêt")
                return None
            
            # Vérifier le signal M1 (SANS GALE)
            result, details = await self._verify_signal_m1(
                signal_id, pair, direction, ts_enter
            )
            
            if result:
                self._update_signal_result(signal_id, result, details)
                emoji = "✅" if result == 'WIN' else "❌"
                print(f"{emoji} Résultat M1: {result}")
                
                if details and details.get('pips'):
                    print(f"   📊 {details['pips']:.1f} pips")
                
                return result
            else:
                print(f"⚠️ Impossible de vérifier")
                return None
                
        except Exception as e:
            print(f"❌ Erreur verify_single_signal: {e}")
            import traceback
            traceback.print_exc()
            return None

    async def verify_pending_signals(self):
        """Vérifie tous les signaux M1 qui n'ont pas encore de résultat"""
        try:
            now_utc = datetime.now(timezone.utc)
            print("\n" + "="*60)
            print(f"🔍 VÉRIFICATION AUTO M1 - {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print("="*60)

            query = text("""
                SELECT id, pair, direction, ts_enter, confidence
                FROM signals
                WHERE result IS NULL
                ORDER BY ts_enter DESC
                LIMIT 50
            """)
            
            with self.engine.connect() as conn:
                pending = conn.execute(query).fetchall()
            
            print(f"📊 Signaux M1 sans résultat: {len(pending)}")
            
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
                    print(f"🔎 Signal M1 #{signal_id} - {pair} {direction}")
                    print(f"{'='*40}")
                    
                    # Vérifier si week-end
                    if self._is_weekend(ts_enter):
                        print(f"🏖️ Signal du week-end - Marqué comme LOSE")
                        self._update_signal_result(signal_id, 'LOSE', {
                            'entry_price': 0,
                            'exit_price': 0,
                            'pips': 0,
                            'gale_level': 0,
                            'reason': 'Marché fermé (week-end)'
                        })
                        verified_count += 1
                        results.append({
                            'signal_id': signal_id,
                            'pair': pair,
                            'direction': direction,
                            'result': 'LOSE',
                            'details': {'reason': 'Week-end'},
                            'confidence': confidence
                        })
                        continue
                    
                    # Vérifier si signal M1 complet
                    if not self._is_signal_complete_m1(ts_enter):
                        skipped_count += 1
                        print(f"➡️  SKIP - Signal M1 pas prêt\n")
                        continue
                    
                    print(f"✅ Signal M1 prêt pour vérification")
                    
                    # Vérifier le signal M1
                    result, details = await self._verify_signal_m1(
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
                        if details and details.get('pips'):
                            print(f"   📊 {details['pips']:.1f} pips")
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
                error_msg = f"❌ **Erreur vérification M1**\n\n{str(e)[:200]}"
                for chat_id in self.admin_chat_ids:
                    try:
                        await self.bot.send_message(chat_id=chat_id, text=error_msg)
                    except:
                        pass

    def _is_signal_complete_m1(self, ts_enter):
        """
        Vérifie si signal M1 est complet
        Pour M1: seulement 1 minute d'attente après l'entrée
        """
        try:
            # Parser timestamp
            if isinstance(ts_enter, str):
                ts_clean = ts_enter.replace('Z', '').replace('+00:00', '').split('.')[0]
                try:
                    entry_time_utc = datetime.fromisoformat(ts_clean)
                except:
                    try:
                        entry_time_utc = datetime.strptime(ts_clean, '%Y-%m-%d %H:%M:%S')
                    except:
                        print(f"   ❌ Format timestamp invalide: {ts_enter}")
                        return False
            else:
                entry_time_utc = ts_enter
            
            # S'assurer que c'est en UTC
            if entry_time_utc.tzinfo is None:
                entry_time_utc = entry_time_utc.replace(tzinfo=timezone.utc)
            else:
                entry_time_utc = entry_time_utc.astimezone(timezone.utc)

            # Pour M1: vérification 1 minute après l'entrée
            end_time_utc = entry_time_utc + timedelta(minutes=1)
            
            now_utc = datetime.now(timezone.utc)
            
            is_complete = now_utc >= end_time_utc
            
            print(f"   📅 Entrée UTC: {entry_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📅 Fin M1 UTC: {end_time_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   📅 Maintenant UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   ⏱️  Temps restant: {(end_time_utc - now_utc).total_seconds():.0f}s")
            print(f"   {'✅ COMPLET M1' if is_complete else '⏳ PAS COMPLET M1'}")
            
            return is_complete
            
        except Exception as e:
            print(f"❌ Erreur _is_signal_complete_m1: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def _verify_signal_m1(self, signal_id, pair, direction, ts_enter):
        """
        Vérifie signal M1 SANS GALE
        Une seule tentative - Win ou Lose immédiat
        """
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
            
            # Vérifier si week-end
            if self._is_weekend(entry_time_utc):
                print(f"   🏖️ Signal du week-end - Marché fermé")
                return 'LOSE', {
                    'entry_price': 0,
                    'exit_price': 0,
                    'pips': 0,
                    'gale_level': 0,
                    'reason': 'Marché fermé (week-end)'
                }

            # Pour M1: entrée = maintenant, sortie = 1 minute après
            exit_time_utc = entry_time_utc + timedelta(minutes=1)
            
            print(f"   📍 M1 Trading: {entry_time_utc.strftime('%H:%M')} → {exit_time_utc.strftime('%H:%M')} UTC")
            
            # Récupérer prix d'entrée
            entry_price = await self._get_price_at_time(pair, entry_time_utc)
            if entry_price is None:
                print(f"   ⚠️  Prix d'entrée M1 non disponible")
                return None, None
            
            await asyncio.sleep(0.5)
            
            # Récupérer prix de sortie (1 minute après)
            exit_price = await self._get_price_at_time(pair, exit_time_utc)
            if exit_price is None:
                print(f"   ⚠️  Prix de sortie M1 non disponible")
                return None, None
            
            # Calculer résultat
            if direction == 'CALL':
                is_winning = exit_price > entry_price
            else:  # PUT
                is_winning = exit_price < entry_price
            
            pips_diff = abs(exit_price - entry_price) * 10000
            
            print(f"   💰 Entrée: {entry_price:.5f} | Sortie: {exit_price:.5f}")
            print(f"   📊 Différence: {pips_diff:.1f} pips")
            
            result = 'WIN' if is_winning else 'LOSE'
            
            details = {
                'entry_price': entry_price,
                'exit_price': exit_price,
                'pips': pips_diff,
                'gale_level': 0  # SANS GALE
            }
            
            if is_winning:
                print(f"   ✅ WIN M1 (+{pips_diff:.1f} pips)")
            else:
                print(f"   ❌ LOSE M1 (-{pips_diff:.1f} pips)")
            
            return result, details
            
        except Exception as e:
            print(f"❌ Erreur _verify_signal_m1: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    async def _get_price_at_time(self, pair, timestamp):
        """Récupère prix à un moment donné (timestamp en UTC) pour M1"""
        try:
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            ts_utc = timestamp.astimezone(timezone.utc)
            
            # Vérifier si week-end
            if self._is_weekend(ts_utc):
                print(f"   🏖️ Week-end détecté - Pas d'appel API")
                return None
            
            # Plage réduite pour M1: ±2 minutes
            start_dt = ts_utc - timedelta(minutes=2)
            end_dt = ts_utc + timedelta(minutes=2)
            
            start_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
            
            params = {
                'symbol': pair,
                'interval': '1min',
                'outputsize': 5,  # Très réduit pour M1
                'apikey': self.api_key,
                'format': 'JSON',
                'start_date': start_str,
                'end_date': end_str
            }
            
            print(f"   🔍 API M1: {pair} autour de {ts_utc.strftime('%H:%M:%S')} UTC")
            
            resp = self._session.get(self.base_url, params=params, timeout=12)
            resp.raise_for_status()
            data = resp.json()
            
            # Vérifier limite API
            if 'code' in data and data['code'] == 429:
                print(f"   ⚠️  LIMITE API ATTEINTE")
                return None
            
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
                
                # Pour M1: tolérance de 2 minutes max
                if closest_candle and min_diff <= 120:
                    try:
                        price = float(closest_candle['close'])
                        print(f"   💰 Prix trouvé: {price} (diff: {min_diff:.0f}s)")
                        return price
                    except:
                        return None
            
            print(f"   ⚠️  Aucune bougie M1 trouvée pour {pair}")
            return None
            
        except Exception as e:
            print(f"⚠️  Erreur API M1 pour {pair}: {e}")
            return None

    def _update_signal_result(self, signal_id, result, details):
        """Met à jour résultat dans DB"""
        try:
            gale_level = 0  # Toujours 0 en mode SANS GALE
            reason = details.get('reason', '') if details else ''
            
            query = text("""
                UPDATE signals
                SET result = :result, gale_level = :gale_level, reason = :reason
                WHERE id = :id
            """)
            
            with self.engine.begin() as conn:
                conn.execute(query, {
                    'result': result,
                    'gale_level': gale_level,
                    'reason': reason,
                    'id': signal_id
                })
            
            print(f"💾 Résultat M1 sauvegardé: #{signal_id} = {result}")
            
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

        msg = "📊 **RAPPORT VÉRIFICATION M1**\n"
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
            msg += f"\n🎯 Mode: M1 SANS GALE\n"
        
        msg += "\n━━━━━━━━━━━━━━━━━━━━"
        
        for chat_id in self.admin_chat_ids:
            try:
                await self.bot.send_message(chat_id=chat_id, text=msg)
            except Exception as e:
                print(f"❌ Envoi à {chat_id}: {e}")

    async def _send_verification_report(self, results, skipped_count=0, error_count=0):
        """Envoie rapport de vérification M1"""
        try:
            print("📝 Génération rapport M1...")

            today_stats = self._get_today_stats()
            wins = sum(1 for r in results if r.get('result') == 'WIN')
            losses = len(results) - wins
            
            report = "📊 **RAPPORT VÉRIFICATION M1**\n"
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
                report += "\n📋 **Détails M1:**\n\n"
                
                for i, r in enumerate(results[:10], 1):
                    emoji = "✅" if r['result'] == 'WIN' else "❌"
                    
                    detail_text = ""
                    if r['details'].get('reason'):
                        detail_text = f" • {r['details']['reason']}"
                    elif r['details'].get('pips'):
                        detail_text = f" • {r['details']['pips']:.1f} pips"
                    
                    report += f"{i}. {emoji} **{r['pair']}** {r['direction']}{detail_text}\n"
            else:
                report += "ℹ️ Aucun signal vérifié\n"
                if skipped_count > 0:
                    report += f"\n⏳ {skipped_count} signal(s) M1 en attente\n"
            
            report += f"\n🎯 Mode: M1 SANS GALE\n"
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
            
            print(f"✅ Rapport M1 envoyé à {sent_count}/{len(self.admin_chat_ids)}")
            
        except Exception as e:
            print(f"❌ Erreur rapport M1: {e}")
            import traceback
            traceback.print_exc()

    def _get_today_stats(self):
        """Stats du jour UNIQUEMENT - basé sur ts_send en heure Haïti"""
        try:
            from zoneinfo import ZoneInfo
            HAITI_TZ = ZoneInfo("America/Port-au-Prince")
            
            now_haiti = datetime.now(HAITI_TZ)
            start_haiti = now_haiti.replace(hour=0, minute=0, second=0, microsecond=0)
            end_haiti = start_haiti + timedelta(days=1)
            
            start_utc = start_haiti.astimezone(timezone.utc)
            end_utc = end_haiti.astimezone(timezone.utc)

            query = text("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result = 'LOSE' THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
                FROM signals
                WHERE ts_send >= :start AND ts_send < :end
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
