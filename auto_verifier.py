# auto_verifier.py
"""
AutoResultVerifier
Fichier complet et corrigé pour la vérification automatique des résultats.
- Parsing robuste des timestamps
- Vérification avec gales
- Récupération de prix via TwelveData (1min)
- Envoi de rapports Telegram si bot fourni
"""

import asyncio
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from sqlalchemy import text

class AutoResultVerifier:
    def __init__(self, engine, twelvedata_api_key, bot=None):
        self.engine = engine
        self.api_key = twelvedata_api_key
        self.base_url = 'https://api.twelvedata.com/time_series'
        self.bot = bot
        self.admin_chat_ids = []
        self.utc_tz = timezone.utc
        self.local_tz = ZoneInfo("America/Port-au-Prince")

    def set_bot(self, bot):
        """Assigner le bot Telegram pour l'envoi des rapports"""
        self.bot = bot

    def add_admin(self, chat_id):
        """Ajouter un admin qui recevra le rapport"""
        if chat_id not in self.admin_chat_ids:
            self.admin_chat_ids.append(chat_id)
            print(f"✅ Admin {chat_id} ajouté pour recevoir les rapports")

    async def verify_pending_signals(self, limit=50):
        """
        Vérifie tous les signaux sans résultat dont ts_enter est passé.
        """
        try:
            print("\n" + "="*60)
            print(f"🔍 VÉRIFICATION AUTOMATIQUE - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print("="*60)

            query = text("""
                SELECT id, pair, direction, ts_enter, confidence,
                       COALESCE(timeframe, 5) as timeframe,
                       COALESCE(gale_level, 0) as gale_level,
                       COALESCE(max_gales, 2) as max_gales
                FROM signals
                WHERE result IS NULL
                ORDER BY ts_enter DESC
                LIMIT :limit
            """)

            with self.engine.connect() as conn:
                rows = conn.execute(query, {'limit': limit}).fetchall()

            print(f"📌 Rows fetched for pending check: {len(rows)}")

            pending = []
            now_utc = datetime.now(timezone.utc)

            for row in rows:
                # row: (id, pair, direction, ts_enter, confidence, timeframe, gale_level, max_gales)
                try:
                    sid = row[0]
                    pair = row[1]
                    direction = row[2]
                    ts_enter_raw = row[3]
                    confidence = row[4]
                    timeframe = int(row[5] or 5)
                    max_gales = int(row[7]) if len(row) > 7 and row[7] is not None else 2
                except Exception as e:
                    print(f"⚠️  Ligne DB mal formée, skip: {row} ({e})")
                    continue

                if not ts_enter_raw:
                    print(f"⚠️  ts_enter vide pour signal {sid}, skip")
                    continue

                # Parse ts_enter robustly
                entry_time = None
                raw = str(ts_enter_raw)
                parsed = False
                try:
                    entry_time = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                    if entry_time.tzinfo is None:
                        entry_time = entry_time.replace(tzinfo=timezone.utc)
                    parsed = True
                except Exception:
                    parsed = False

                if not parsed:
                    try:
                        entry_time = datetime.strptime(raw, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                        parsed = True
                    except Exception:
                        parsed = False

                if not parsed:
                    try:
                        entry_time = datetime.fromisoformat(raw)
                        if entry_time.tzinfo is None:
                            entry_time = entry_time.replace(tzinfo=timezone.utc)
                        parsed = True
                    except Exception:
                        parsed = False

                if not parsed:
                    print(f"⚠️ Impossible de parser ts_enter pour signal {sid}: {ts_enter_raw}")
                    continue

                # On garde uniquement les signaux dont l'entrée est passée
                if entry_time <= now_utc:
                    class SignalRow:
                        def __init__(self, sid, pair, direction, ts_iso, confidence, timeframe, max_gales):
                            self.id = sid
                            self.pair = pair
                            self.direction = direction
                            self.ts_enter = ts_iso
                            self.confidence = confidence
                            self.timeframe = timeframe
                            self.max_gales = max_gales

                    pending.append(SignalRow(sid, pair, direction, entry_time.isoformat(), confidence, timeframe, max_gales))

            print(f"📊 Signaux trouvés: {len(pending)}")

            if not pending:
                print("✅ Aucun signal en attente de vérification")
                print("="*60 + "\n")
                # Envoyer rapport succinct aux admins si présents
                if self.bot and self.admin_chat_ids:
                    today_stats = self._get_today_stats()
                    if today_stats and today_stats['total_signals'] > 0:
                        msg = "📊 **RAPPORT DE VÉRIFICATION**\n━━━━━━━━━━━━━━━━━━━━\n\n"
                        msg += "✅ Aucun signal à vérifier maintenant\n\n"
                        msg += f"📅 **Statistiques du jour:**\n• Total signaux: {today_stats['total_signals']}\n"
                        msg += f"• ✅ Réussis: {today_stats['wins']}\n• ❌ Échoués: {today_stats['losses']}\n• ⏳ En attente: {today_stats['pending']}\n"
                        if today_stats['wins'] + today_stats['losses'] > 0:
                            msg += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"
                        msg += "\n━━━━━━━━━━━━━━━━━━━━"
                    else:
                        msg = "📊 **RAPPORT DE VÉRIFICATION**\n━━━━━━━━━━━━━━━━━━━━\n\n✅ Aucun signal à vérifier\n\nℹ️ Aucun signal n'a été envoyé aujourd'hui\n\n━━━━━━━━━━━━━━━━━━━━"

                    for chat_id in self.admin_chat_ids:
                        try:
                            await self.bot.send_message(chat_id=chat_id, text=msg)
                            print(f"✅ Rapport envoyé à {chat_id}")
                        except Exception as e:
                            print(f"⚠️ Erreur envoi rapport à {chat_id}: {e}")
                return

            results = []
            verified_count = 0
            skipped_count = 0
            error_count = 0

            for signal in pending:
                try:
                    if not self._is_signal_complete(signal):
                        skipped_count += 1
                        continue

                    print(f"\n🔎 Signal #{signal.id} - {signal.pair} {signal.direction} M{signal.timeframe}")
                    result, details = await self._verify_signal_with_gales(signal)

                    if result:
                        self._update_signal_result(signal.id, result, details)
                        verified_count += 1
                        results.append({'signal': signal, 'result': result, 'details': details})

                        emoji = "✅" if result == 'WIN' else "❌"
                        print(f"{emoji} Résultat: {result}")
                        if details and details.get('winning_attempt'):
                            print(f"   Gagné à: {details['winning_attempt']}")
                        if details:
                            if 'entry_price' in details and 'exit_price' in details:
                                print(f"   Entrée: {details['entry_price']:.5f}")
                                print(f"   Sortie: {details['exit_price']:.5f}")
                            if 'pips' in details:
                                print(f"   Diff: {details['pips']:.1f} pips")
                    else:
                        error_count += 1
                        print(f"⚠️  Impossible de vérifier le signal #{signal.id}")

                    # petite pause pour limiter la cadence vers l'API
                    await asyncio.sleep(1.0)

                except Exception as e:
                    error_count += 1
                    print(f"❌ Erreur vérification signal {signal.id}: {e}")
                    import traceback
                    traceback.print_exc()

            print("\n" + "-"*60)
            print(f"📈 RÉSUMÉ: {verified_count} vérifiés, {skipped_count} en attente, {error_count} erreurs")
            print("="*60 + "\n")

            # Envoi du rapport aux admins (si bot configuré)
            if self.bot and self.admin_chat_ids:
                print(f"📤 Envoi du rapport à {len(self.admin_chat_ids)} admin(s)")
                await self._send_verification_report(results, skipped_count, error_count)
            else:
                print(f"⚠️ Impossible d'envoyer le rapport: Bot configuré: {self.bot is not None}; Admins: {len(self.admin_chat_ids)}")

            if verified_count > 0:
                self._check_ml_retraining()

        except Exception as e:
            print(f"❌ ERREUR GLOBALE dans verify_pending_signals: {e}")
            import traceback
            traceback.print_exc()
            if self.bot and self.admin_chat_ids:
                err_text = f"❌ **Erreur lors de la vérification**\n\n{str(e)}"
                for chat_id in self.admin_chat_ids:
                    try:
                        await self.bot.send_message(chat_id=chat_id, text=err_text)
                    except Exception:
                        pass

    def _is_signal_complete(self, signal):
        """
        Vérifie si toutes les tentatives (signal + gales) ont leurs fenêtres temporelles terminées.
        """
        try:
            try:
                entry_time = datetime.fromisoformat(signal.ts_enter.replace('Z', '+00:00'))
            except Exception:
                entry_time = datetime.fromisoformat(signal.ts_enter)
                if entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=timezone.utc)
        except Exception as e:
            print(f"⚠️ _is_signal_complete: impossible de parser ts_enter {signal.ts_enter}: {e}")
            return False

        timeframe = int(signal.timeframe or 5)
        max_attempts = int(signal.max_gales) + 1 if hasattr(signal, 'max_gales') else 3

        total_time_needed = timeframe * max_attempts
        last_attempt_end = entry_time + timedelta(minutes=total_time_needed)

        now = datetime.now(timezone.utc)
        is_complete = now >= last_attempt_end

        if not is_complete:
            time_remaining = (last_attempt_end - now).total_seconds() / 60
            print(f"⏳ Signal #{signal.id} pas encore terminé (reste {time_remaining:.1f} min)")

        return is_complete

    async def _verify_signal_with_gales(self, signal):
        """
        Vérifie un signal en testant chaque tentative (signal initial + gales).
        """
        try:
            try:
                entry_time = datetime.fromisoformat(signal.ts_enter.replace('Z', '+00:00'))
            except Exception:
                entry_time = datetime.fromisoformat(signal.ts_enter)
                if entry_time.tzinfo is None:
                    entry_time = entry_time.replace(tzinfo=timezone.utc)
        except Exception as e:
            print(f"⚠️ _verify_signal_with_gales: parsing ts_enter failed for {signal.ts_enter}: {e}")
            return None, None

        timeframe = int(signal.timeframe or 5)
        max_attempts = int(signal.max_gales) + 1 if getattr(signal, 'max_gales', None) is not None else 3

        last_entry_price = None
        last_exit_price = None
        last_pips_diff = 0

        for attempt in range(max_attempts):
            attempt_entry = entry_time + timedelta(minutes=timeframe * attempt)
            attempt_exit = attempt_entry + timedelta(minutes=timeframe)

            print(f"   Tentative {attempt + 1}/{max_attempts}: {attempt_entry.strftime('%Y-%m-%d %H:%M:%S')} UTC")

            entry_price = await self._get_price_at_time(signal.pair, attempt_entry)
            if entry_price is None:
                print(f"   ⚠️  Prix d'entrée non disponible pour {attempt_entry}")
                continue

            await asyncio.sleep(0.5)

            exit_price = await self._get_price_at_time(signal.pair, attempt_exit)
            if exit_price is None:
                print(f"   ⚠️  Prix de sortie non disponible pour {attempt_exit}")
                last_entry_price = entry_price
                continue

            last_entry_price = entry_price
            last_exit_price = exit_price

            if (signal.direction or "").upper() == 'CALL':
                is_winning = exit_price > entry_price
            else:
                is_winning = exit_price < entry_price

            pips_diff = abs(exit_price - entry_price) * 10000
            last_pips_diff = pips_diff

            if is_winning:
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

        print(f"   ❌ LOSE après {max_attempts} tentatives")

        if last_entry_price is None or last_exit_price is None:
            print(f"   ⚠️  Impossible de récupérer des prix fiables pour signal {signal.id}")
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
        """
        Récupère le prix 'close' le plus proche du timestamp via TwelveData (1min interval).
        """
        try:
            start_dt = timestamp - timedelta(minutes=3)
            end_dt = timestamp + timedelta(minutes=2)
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

            resp = requests.get(self.base_url, params=params, timeout=12)
            resp.raise_for_status()
            data = resp.json()

            if 'values' in data and len(data['values']) > 0:
                closest = None
                min_diff = float('inf')
                for candle in data['values']:
                    try:
                        candle_time = datetime.strptime(candle['datetime'], '%Y-%m-%d %H:%M:%S')
                        candle_time = candle_time.replace(tzinfo=timezone.utc)
                        diff = abs((candle_time - timestamp).total_seconds())
                        if diff < min_diff:
                            min_diff = diff
                            closest = candle
                    except Exception:
                        continue
                if closest and min_diff < 180:
                    return float(closest['close'])
            return None

        except requests.HTTPError as he:
            print(f"⚠️ HTTP error TwelveData: {he} (pair={pair})")
            return None
        except Exception as e:
            print(f"⚠️ Erreur _get_price_at_time: {e} (pair={pair})")
            return None

    def _update_signal_result(self, signal_id, result, details):
        """
        Met à jour la table signals: result, ts_result, winning_attempt, gale_level si disponible.
        """
        try:
            gale_level = None
            if details and details.get('attempt_number') is not None:
                try:
                    gale_level = int(details.get('attempt_number')) - 1
                except Exception:
                    gale_level = None

            gale_sql = ", gale_level = :gale_level" if gale_level is not None else ""

            query = text(f"""
                UPDATE signals
                SET result = :result,
                    ts_result = :ts_result,
                    winning_attempt = :winning_attempt
                    {gale_sql}
                WHERE id = :id
            """)

            params = {
                'result': result,
                'ts_result': datetime.utcnow().isoformat(),
                'winning_attempt': details.get('winning_attempt') if details else None,
                'id': signal_id
            }
            if gale_level is not None:
                params['gale_level'] = gale_level

            with self.engine.begin() as conn:
                conn.execute(query, params)

            print(f"💾 Résultat sauvegardé: Signal #{signal_id} = {result}")

        except Exception as e:
            print(f"❌ Erreur _update_signal_result pour {signal_id}: {e}")

    async def _send_verification_report(self, results, skipped_count=0, error_count=0):
        """
        Construit et envoie un rapport synthétique aux admins (max 10 détails).
        """
        try:
            print("📝 Génération du rapport...")
            today_stats = self._get_today_stats()

            wins = sum(1 for r in results if r['result'] == 'WIN')
            losses = len(results) - wins

            report = "📊 **RAPPORT DE VÉRIFICATION**\n"
            report += "━━━━━━━━━━━━━━━━━━━━\n\n"

            if today_stats and today_stats['total_signals'] > 0:
                report += f"📅 **Statistiques du jour:**\n"
                report += f"• Total signaux: {today_stats['total_signals']}\n"
                report += f"• ✅ Réussis: {today_stats['wins']}\n"
                report += f"• ❌ Échoués: {today_stats['losses']}\n"
                report += f"• ⏳ En attente: {today_stats['pending']}\n"
                if today_stats['wins'] + today_stats['losses'] > 0:
                    report += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"
                report += "\n"

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
                report += "📋 **Détails:**\n\n"

                for i, r in enumerate(results[:10], 1):
                    emoji = "✅" if r['result'] == 'WIN' else "❌"
                    sig = r['signal']
                    det = r['details'] or {}
                    attempt_info = f" • {det.get('winning_attempt')}" if det.get('winning_attempt') else ""
                    pips = det.get('pips', 0)
                    conf = sig.confidence or 0
                    report += f"{i}. {emoji} **{sig.pair}** {sig.direction}{attempt_info}\n"
                    report += f"   📊 {pips:.1f} pips | Confiance: {int(conf*100) if isinstance(conf, (int,float)) else conf}\n\n"
            else:
                report += "ℹ️ Aucun signal vérifié lors de cette session\n"
                if skipped_count > 0:
                    report += f"\n⏳ **{skipped_count} signal(s) en attente**\n   (Le temps nécessaire n'est pas encore écoulé)\n"
                if error_count > 0:
                    report += f"\n⚠️ {error_count} erreur(s) rencontrée(s)\n"

            report += "\n━━━━━━━━━━━━━━━━━━━━"

            sent = 0
            for chat_id in self.admin_chat_ids:
                try:
                    await self.bot.send_message(chat_id=chat_id, text=report, parse_mode='Markdown')
                    sent += 1
                except Exception as e:
                    print(f"❌ Erreur envoi rapport à {chat_id}: {e}")

            print(f"📤 Rapport envoyé à {sent}/{len(self.admin_chat_ids)} admin(s)")

        except Exception as e:
            print(f"❌ ERREUR dans _send_verification_report: {e}")
            import traceback
            traceback.print_exc()

    def _get_today_stats(self):
        """
        Calcule les statistiques des signaux du jour en bornes UTC.
        """
        try:
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = today_start + timedelta(days=1)

            q = text("""
                SELECT
                    COUNT(*) as total_signals,
                    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result = 'LOSE' THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN result IS NULL THEN 1 ELSE 0 END) as pending
                FROM signals
                WHERE ts_enter >= :start AND ts_enter < :end
            """)

            with self.engine.connect() as conn:
                stats = conn.execute(q, {
                    'start': today_start.strftime('%Y-%m-%d %H:%M:%S'),
                    'end': today_end.strftime('%Y-%m-%d %H:%M:%S')
                }).fetchone()

            if stats and stats[0] > 0:
                total_signals = int(stats[0] or 0)
                wins = int(stats[1] or 0)
                losses = int(stats[2] or 0)
                pending = int(stats[3] or 0)
                verified = wins + losses
                winrate = (wins / verified * 100) if verified > 0 else 0.0
                return {
                    'total_signals': total_signals,
                    'wins': wins,
                    'losses': losses,
                    'pending': pending,
                    'winrate': winrate
                }
            return None

        except Exception as e:
            print(f"⚠️ Erreur _get_today_stats: {e}")
            return None

    def _check_ml_retraining(self):
        try:
            q = text("SELECT COUNT(*) as count FROM signals WHERE result IS NOT NULL")
            with self.engine.connect() as conn:
                count = conn.execute(q).scalar()
            if count and count >= 100 and count % 50 == 0:
                print(f"\n🎓 {count} résultats disponibles")
                print("💡 Réentraînement du modèle ML recommandé (utilisez /train)\n")
        except Exception as e:
            print(f"⚠️ Erreur _check_ml_retraining: {e}")

    def get_performance_stats(self):
        try:
            q = text("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result = 'LOSE' THEN 1 ELSE 0 END) as losses,
                    AVG(confidence) as avg_confidence
                FROM signals
                WHERE result IS NOT NULL
            """)
            with self.engine.connect() as conn:
                stats = conn.execute(q).fetchone()
            if stats and stats[0] > 0:
                total = int(stats[0])
                wins = int(stats[1] or 0)
                losses = int(stats[2] or 0)
                winrate = (wins / total) * 100 if total > 0 else 0.0
                avg_conf = float(stats[3] or 0.0)
                return {'total': total, 'wins': wins, 'losses': losses, 'winrate': winrate, 'avg_confidence': avg_conf}
            return None
        except Exception as e:
            print(f"⚠️ Erreur get_performance_stats: {e}")
            return None

    def get_recent_results(self, limit=10):
        try:
            q = text("""
                SELECT pair, direction, result, confidence,
                       COALESCE(timeframe, 5) as timeframe,
                       winning_attempt, ts_enter, ts_result
                FROM signals
                WHERE result IS NOT NULL
                ORDER BY ts_result DESC
                LIMIT :limit
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(q, {'limit': limit}).fetchall()
            return rows
        except Exception as e:
            print(f"⚠️ Erreur get_recent_results: {e}")
            return []

    async def send_daily_summary(self):
        try:
            stats = self._get_today_stats()
            if not stats or stats['total_signals'] == 0:
                return
            report = "📊 **RÉSUMÉ QUOTIDIEN**\n━━━━━━━━━━━━━━━━━━━━\n\n"
            report += f"📅 Date: {datetime.utcnow().strftime('%d/%m/%Y')}\n\n"
            report += f"📈 **Résultats:**\n"
            report += f"• Total signaux: {stats['total_signals']}\n"
            report += f"• ✅ Réussis: {stats['wins']}\n"
            report += f"• ❌ Échoués: {stats['losses']}\n"
            report += f"• ⏳ En attente: {stats['pending']}\n\n"
            if stats['wins'] + stats['losses'] > 0:
                report += f"• Win rate: {stats['winrate']:.1f}%\n"
                if stats['winrate'] >= 70:
                    report += "• 🎉 Excellente performance !\n"
                elif stats['winrate'] >= 60:
                    report += "• 👍 Bonne performance\n"
                else:
                    report += "• ⚠️ Performance à améliorer\n"
            report += "\n━━━━━━━━━━━━━━━━━━━━"
            for chat_id in self.admin_chat_ids:
                try:
                    await self.bot.send_message(chat_id=chat_id, text=report)
                except Exception as e:
                    print(f"⚠️  Erreur envoi résumé à {chat_id}: {e}")
        except Exception as e:
            print(f"⚠️ Erreur send_daily_summary: {e}")
