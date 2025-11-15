# auto_verifier.py
"""
AutoResultVerifier
Gère correctement les fuseaux horaires UTC <-> Haïti et vérifie les signaux avec gales.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from sqlalchemy import text
import requests

LOCAL_TZ = ZoneInfo("America/Port-au-Prince")

class AutoResultVerifier:
    def __init__(self, engine, twelvedata_api_key, bot=None):
        self.engine = engine
        self.api_key = twelvedata_api_key
        self.base_url = 'https://api.twelvedata.com/time_series'
        self.bot = bot
        self.admin_chat_ids = []

        # paramètres
        self.default_timeframe = 5      # minutes par tentative
        self.default_max_gales = 2      # 2 gales => 3 tentatives total

        # session HTTP réutilisable
        self._session = requests.Session()

    def set_bot(self, bot):
        self.bot = bot
        print("✅ Bot configuré pour notifications")

    def add_admin(self, chat_id):
        if chat_id not in self.admin_chat_ids:
            self.admin_chat_ids.append(chat_id)
            print(f"✅ Admin {chat_id} ajouté")

    async def verify_pending_signals(self):
        """Vérifie tous les signaux sans résultat."""
        try:
            now_local = datetime.now(LOCAL_TZ)
            print("\n" + "="*60)
            print(f"🔍 VÉRIFICATION - {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")
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

            print(f"📊 Signaux sans résultat: {len(pending)}")
            if not pending:
                print("✅ Aucun signal en attente")
                print("="*60 + "\n")
                if self.bot and self.admin_chat_ids:
                    await self._send_no_pending_report()
                return

            results = []
            verified_count = skipped_count = error_count = 0

            for row in pending:
                try:
                    signal_id, pair, direction, ts_enter, confidence = row
                    confidence = confidence if confidence else 0.5

                    print(f"\n{'='*40}")
                    print(f"🔎 Signal #{signal_id} - {pair} {direction}")
                    print(f"{'='*40}")

                    if not self._is_signal_complete(ts_enter):
                        skipped_count += 1
                        print("⏭️ SKIP - Pas encore prêt\n")
                        continue

                    print("✅ Prêt pour vérification")
                    result, details = await self._verify_signal_with_gales(signal_id, pair, direction, ts_enter)

                    if result:
                        self._update_signal_result(signal_id, result, details)
                        verified_count += 1

                        emoji = "✅" if result == 'WIN' else "❌"
                        print(f"{emoji} Résultat: {result}")
                        if details and details.get('gale_level') is not None:
                            gale_names = ["Signal initial", "Gale 1", "Gale 2"]
                            lev = details.get('gale_level')
                            if isinstance(lev, int) and lev < len(gale_names):
                                print(f"   Niveau: {gale_names[lev]}")
                        results.append({
                            'signal_id': signal_id,
                            'pair': pair,
                            'direction': direction,
                            'result': result,
                            'details': details or {},
                            'confidence': confidence
                        })
                    else:
                        error_count += 1
                        print(f"⚠️ Impossible de vérifier #{signal_id}")

                    await asyncio.sleep(1.5)  # pause entre signaux

                except Exception as e:
                    error_count += 1
                    print(f"❌ Erreur interne: {e}")
                    import traceback
                    traceback.print_exc()

            print("\n" + "-"*60)
            print(f"📈 RÉSUMÉ: {verified_count} vérifiés, {skipped_count} en attente, {error_count} erreurs")
            print("="*60 + "\n")

            if self.bot and self.admin_chat_ids:
                await self._send_verification_report(results, skipped_count, error_count)

        except Exception as e:
            print(f"❌ ERREUR GLOBALE: {e}")
            import traceback
            traceback.print_exc()
            if self.bot and self.admin_chat_ids:
                error_msg = f"❌ **Erreur de vérification**\n\n{str(e)[:200]}"
                for cid in self.admin_chat_ids:
                    try:
                        await self.bot.send_message(chat_id=cid, text=error_msg)
                    except:
                        pass

    def _parse_ts(self, ts_str):
        """Parse ISO timestamp en datetime timezone-aware (assume UTC si pas d'info)."""
        if ts_str is None:
            return None
        try:
            # support 'Z' suffix
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        except Exception:
            try:
                dt = datetime.fromisoformat(ts_str)
            except Exception:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _is_signal_complete(self, ts_enter):
        """Retourne True si toutes les tentatives sont terminées (calcul en temps local)."""
        try:
            entry_time = self._parse_ts(ts_enter)
            if entry_time is None:
                print("❌ Impossible de parser ts_enter")
                return False

            entry_local = entry_time.astimezone(LOCAL_TZ)
            total_minutes = self.default_timeframe * (self.default_max_gales + 1)
            end_local = entry_local + timedelta(minutes=total_minutes)
            now_local = datetime.now(LOCAL_TZ)

            print(f"   📅 Entrée: {entry_local.strftime('%Y-%m-%d %H:%M %Z')}")
            print(f"   📅 Fin prévue: {end_local.strftime('%Y-%m-%d %H:%M %Z')}")
            print(f"   📅 Maintenant: {now_local.strftime('%Y-%m-%d %H:%M %Z')}")

            is_complete = now_local >= end_local
            if not is_complete:
                time_left = (end_local - now_local).total_seconds() / 60
                print(f"   ⏳ Reste {time_left:.1f} min")
            else:
                print("   ✅ COMPLET")
            return is_complete

        except Exception as e:
            print(f"❌ Erreur _is_signal_complete: {e}")
            return False

    async def _verify_signal_with_gales(self, signal_id, pair, direction, ts_enter):
        """Teste chaque tentative (initial + gales). Retourne (result, details) ou (None, None)."""
        try:
            entry_time = self._parse_ts(ts_enter)
            if entry_time is None:
                print("❌ ts_enter invalide")
                return None, None

            max_attempts = self.default_max_gales + 1
            last_entry_price = last_exit_price = None
            last_pips = 0

            for attempt in range(max_attempts):
                attempt_entry = entry_time + timedelta(minutes=self.default_timeframe * attempt)
                attempt_exit = attempt_entry + timedelta(minutes=self.default_timeframe)

                # affiche en local pour lisibilité
                print(f"   Tentative {attempt+1}/{max_attempts} - entrée (local): {attempt_entry.astimezone(LOCAL_TZ).strftime('%Y-%m-%d %H:%M %Z')}")

                entry_price = await self._get_price_at_time(pair, attempt_entry)
                if entry_price is None:
                    print("   ⚠️ Prix d'entrée introuvable")
                    continue

                await asyncio.sleep(0.5)
                exit_price = await self._get_price_at_time(pair, attempt_exit)
                if exit_price is None:
                    print("   ⚠️ Prix de sortie introuvable")
                    last_entry_price = entry_price
                    continue

                last_entry_price = entry_price
                last_exit_price = exit_price

                is_winning = (exit_price > entry_price) if direction == 'CALL' else (exit_price < entry_price)
                pips = abs(exit_price - entry_price) * 10000
                last_pips = pips

                if is_winning:
                    print(f"   ✅ WIN (+{pips:.1f} pips)")
                    return 'WIN', {
                        'entry_price': entry_price,
                        'exit_price': exit_price,
                        'pips': pips,
                        'gale_level': attempt
                    }
                else:
                    print(f"   ❌ Tentative {attempt+1} perdue ({pips:.1f} pips)")

            # pas de victoire après toutes les tentatives
            print(f"   ❌ LOSE après {max_attempts} tentatives")
            if last_entry_price is None or last_exit_price is None:
                print("   ⚠️ Données insuffisantes pour trancher")
                return None, None

            return 'LOSE', {
                'entry_price': last_entry_price,
                'exit_price': last_exit_price,
                'pips': last_pips,
                'gale_level': None
            }

        except Exception as e:
            print(f"❌ Erreur _verify_signal_with_gales: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    async def _get_price_at_time(self, pair, timestamp):
        """
        Récupère le prix le plus proche d'un timestamp fourni.
        `timestamp` doit être timezone-aware (idéalement UTC).
        """
        try:
            if timestamp is None:
                return None
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            ts_utc = timestamp.astimezone(timezone.utc)

            start_dt = ts_utc - timedelta(minutes=3)
            end_dt = ts_utc + timedelta(minutes=2)

            params = {
                'symbol': pair,
                'interval': '1min',
                'outputsize': 10,
                'apikey': self.api_key,
                'format': 'JSON',
                'start_date': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'end_date': end_dt.strftime('%Y-%m-%d %H:%M:%S')
            }

            resp = self._session.get(self.base_url, params=params, timeout=12)
            resp.raise_for_status()
            data = resp.json()

            if 'values' in data and data['values']:
                closest = None
                min_diff = float('inf')
                for candle in data['values']:
                    # parse candle time (TwelveData renvoie habituellement sans tz)
                    try:
                        ct = datetime.fromisoformat(candle['datetime'])
                    except Exception:
                        from datetime import datetime as _dt
                        ct = _dt.strptime(candle['datetime'], '%Y-%m-%d %H:%M:%S')
                    if ct.tzinfo is None:
                        ct = ct.replace(tzinfo=timezone.utc)
                    diff = abs((ct - ts_utc).total_seconds())
                    if diff < min_diff:
                        min_diff = diff
                        closest = candle
                if closest and min_diff <= 180:
                    try:
                        return float(closest['close'])
                    except Exception:
                        return None
            return None

        except Exception as e:
            print(f"⚠️ Erreur API TwelveData: {e}")
            return None

    def _update_signal_result(self, signal_id, result, details):
        """Met à jour le résultat dans la DB (gale_level fallback safe)."""
        try:
            gale_level = 0
            if isinstance(details, dict) and details.get('gale_level') is not None:
                gale_level = details.get('gale_level') if details.get('gale_level') is not None else 0

            query = text("""
                UPDATE signals
                SET result = :result, gale_level = :gale_level
                WHERE id = :id
            """)
            with self.engine.begin() as conn:
                conn.execute(query, {'result': result, 'gale_level': gale_level, 'id': signal_id})
            print(f"💾 Sauvegardé: Signal #{signal_id} = {result}")

        except Exception as e:
            print(f"❌ Erreur update: {e}")
            try:
                query = text("UPDATE signals SET result = :result WHERE id = :id")
                with self.engine.begin() as conn:
                    conn.execute(query, {'result': result, 'id': signal_id})
                print("💾 Sauvegardé (simple)")
            except Exception as e2:
                print(f"❌ Échec total update: {e2}")

    async def _send_no_pending_report(self):
        today_stats = self._get_today_stats()
        msg = "📊 **RAPPORT DE VÉRIFICATION**\n━━━━━━━━━━━━━━━━━━━━\n\n✅ Aucun signal à vérifier\n\n"
        if today_stats and today_stats.get('total_signals', 0) > 0:
            msg += (f"📅 **Stats du jour:**\n• Total: {today_stats['total_signals']}\n"
                    f"• ✅ Réussis: {today_stats['wins']}\n• ❌ Échoués: {today_stats['losses']}\n"
                    f"• ⏳ En attente: {today_stats['pending']}\n")
            if today_stats['wins'] + today_stats['losses'] > 0:
                msg += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"
        msg += "\n━━━━━━━━━━━━━━━━━━━━"
        for cid in self.admin_chat_ids:
            try:
                await self.bot.send_message(chat_id=cid, text=msg)
            except:
                pass

    async def _send_verification_report(self, results, skipped_count=0, error_count=0):
        try:
            today_stats = self._get_today_stats()
            wins = sum(1 for r in results if r.get('result') == 'WIN')
            losses = len(results) - wins

            report = "📊 **RAPPORT DE VÉRIFICATION**\n━━━━━━━━━━━━━━━━━━━━\n\n"
            if today_stats and today_stats.get('total_signals', 0) > 0:
                report += (f"📅 **Stats du jour:**\n• Total: {today_stats['total_signals']}\n"
                           f"• ✅ Réussis: {today_stats['wins']}\n• ❌ Échoués: {today_stats['losses']}\n"
                           f"• ⏳ En attente: {today_stats['pending']}\n")
                if today_stats['wins'] + today_stats['losses'] > 0:
                    report += f"• 📈 Win rate: {today_stats['winrate']:.1f}%\n"
                report += "\n"

            if results:
                report += (f"🔍 **Vérification actuelle:**\n• Vérifiés: {len(results)}\n"
                           f"• ✅ Gains: {wins}\n• ❌ Pertes: {losses}\n")
                if skipped_count:
                    report += f"• ⏳ Non terminés: {skipped_count}\n"
                if error_count:
                    report += f"• ⚠️ Erreurs: {error_count}\n"
                report += "\n📋 **Détails:**\n\n"
                for i, r in enumerate(results[:10], 1):
                    emoji = "✅" if r['result'] == 'WIN' else "❌"
                    details = r.get('details', {})
                    gale_level = details.get('gale_level')
                    gale_text = ""
                    if r['result'] == 'WIN' and gale_level is not None:
                        gale_names = ["Signal initial", "Gale 1", "Gale 2"]
                        if isinstance(gale_level, int) and gale_level < len(gale_names):
                            gale_text = f" • {gale_names[gale_level]}"
                    pips = details.get('pips', 0)
                    report += f"{i}. {emoji} **{r['pair']}** {r['direction']}{gale_text}\n   📊 {pips:.1f} pips\n\n"
            else:
                report += "ℹ️ Aucun signal vérifié\n"
                if skipped_count:
                    report += f"\n⏳ {skipped_count} signal(s) en attente\n"
            report += "\n━━━━━━━━━━━━━━━━━━━━"

            for cid in self.admin_chat_ids:
                try:
                    await self.bot.send_message(chat_id=cid, text=report)
                except:
                    pass

        except Exception as e:
            print(f"❌ Erreur rapport: {e}")

    def _get_today_stats(self):
        """Calcule stats du jour selon date locale Haïti (converties en UTC pour la requête)."""
        try:
            now_local = datetime.now(LOCAL_TZ)
            start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            end_local = start_local + timedelta(days=1)
            start_utc = start_local.astimezone(timezone.utc)
            end_utc = end_local.astimezone(timezone.utc)

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
                stats = conn.execute(query, {"start": start_utc.isoformat(), "end": end_utc.isoformat()}).fetchone()

            if stats and stats[0] > 0:
                total, wins, losses, pending = stats
                wins = wins or 0
                losses = losses or 0
                pending = pending or 0
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
