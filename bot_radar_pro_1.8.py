import os
import time
import datetime
import random
import traceback
import yfinance as yf
import pytz
from threading import Thread, Lock

import indicatori
import stato
import comunicazioni
import qqq_guard
import etoro_engine

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURAZIONE BOT-SPECIFICA
# ─────────────────────────────────────────────────────────────────────────────
ETORO_PUBLIC_KEY  = os.environ.get("ETORO_PUBLIC_KEY",  "").strip()
ETORO_PRIVATE_KEY = os.environ.get("ETORO_PRIVATE_KEY", "").strip()
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN",    "").strip()
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID",  "").strip()
GROQ_API_KEY      = os.environ.get("GROQ_API_KEY",      "").strip()

TICKERS      = ["GOOGL", "NFLX", "CRM", "TSM", "UBER", "BKNG", "SHOP"]
IMPORTO_TEST = 25.0  # Alfa: target reale $25 per trade via by-units
STRUMENTI    = {'GOOGL': 6434, 'NFLX': 1127, 'CRM': 1839, 'TSM': 4481,
                'UBER': 1186, 'BKNG': 1481, 'SHOP': 4148}
MAPPA_ID     = {v: k for k, v in STRUMENTI.items()}
LOG_URL      = "https://script.google.com/macros/s/AKfycbzSWy7ubzt9fE7a_Gx6vgK7ojjuQ9paTBDI1A6SMSP1yZt1zDL159v4vF8qAVeZoa2o/exec"
DAILY_LOSS_LIMIT = -2.5
RICONCILIAZIONE_SEC = 1800 # Riconciliazione posizioni con eToro ogni 30 minuti

print("🚀 RADAR PRO v3.9 - Alfa | SL ATR-based | Fix importo nominale")

# ─────────────────────────────────────────────────────────────────────────────
# 2. STATO GLOBALE
# ─────────────────────────────────────────────────────────────────────────────
START_TIME             = time.time()
SESSION_PROFIT_PERC    = 0.0
SESSION_PROFIT_USD     = 0.0
ULTIMA_SCANSIONE       = "In attesa..."
cooldown_titoli        = {}
blocco_acquisti_fino_a = None
pos_attive             = {}
BOT_PAUSA_MANUALE      = False
CB_NOTIFICATO          = False
LAST_SESSION_DATE      = None
groq_cache             = {}
_ts_riconciliazione    = 0.0  # timestamp ultima riconciliazione posizioni con eToro
execution_lock         = Lock()
state_lock             = Lock()

# ─────────────────────────────────────────────────────────────────────────────
# 3. INIT MODULI CONDIVISI
# ─────────────────────────────────────────────────────────────────────────────
comunicazioni.init(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, LOG_URL)
stato.init("pro_state.json", "PRO", LOG_URL, state_lock)
etoro_engine.init(
    ETORO_PUBLIC_KEY, ETORO_PRIVATE_KEY, STRUMENTI, MAPPA_ID, IMPORTO_TEST,
    state_lock, comunicazioni.invia_telegram, stato.salva_stato, cooldown_min=10
)

# ─────────────────────────────────────────────────────────────────────────────
# 4. LOGICA AI (Groq) — PRO-SPECIFICA
# ─────────────────────────────────────────────────────────────────────────────
import json
import requests as _requests

def interroga_groq(ticker, p, rsi, vwap, vol_ratio, macd_vs_signal, atr_perc, punti_tecnici):
    global groq_cache
    now = time.time()
    if ticker in groq_cache and (now - groq_cache[ticker]['ts']) < 60:
        return groq_cache[ticker]['res'], groq_cache[ticker]['conf']

    if not GROQ_API_KEY:
        print(f"❌ GROQ_API_KEY mancante [{ticker}]")
        return False, 0

    vwap_dist = ((p - vwap) / vwap * 100) if vwap > 0 else 0.0

    prompt = f"""Sei un Senior Risk Manager specializzato in Scalping.
Analizza {ticker}: Prezzo {p:.2f}, RSI {rsi:.1f}, Volumi {vol_ratio:.1f}x, MACD {'Rialzista' if macd_vs_signal else 'Ribassista'}, ATR {atr_perc:.2f}%, Score {punti_tecnici}/4, VWAP dist {vwap_dist:+.2f}%.

MANDATO: Identificare trade con momentum per profitto rapido (max 45 min).
1. RSI > 68 → approva SOLO se Volumi > 1.8x.
2. Volumi < 1.2x → molto scettico (SKIP).
3. Score < 2 → serve motivo eccezionale.
4. VWAP dist positiva = momentum rialzista; negativa = debolezza.

Cerca Asimmetria Rischio/Rendimento.
RISPONDI SOLO JSON: {{"reasoning": "max 10 parole", "decision": "BUY" o "SKIP", "confidence": 1-10}}"""

    try:
        r = _requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.1},
            timeout=10
        )
        if r.status_code == 200:
            testo = r.json()['choices'][0]['message']['content'].strip()
            dati  = json.loads(testo[testo.find('{'):testo.rfind('}')+1])
            res   = (dati.get('decision') == 'BUY')
            conf  = dati.get('confidence', 0)
            groq_cache[ticker] = {'ts': now, 'res': res, 'conf': conf}
            print(f"⚡ Groq [{ticker:4}]: {'✅' if res else '❌'} {dati.get('decision')} (Conf: {conf})")
            return res, conf
        else:
            print(f"❌ Groq [{ticker}] {r.status_code}: {r.text}")
            return None, 0
    except Exception as e:
        print(f"🚨 Groq exception [{ticker}]: {e}")
        return None, 0

def calcola_size_v3(punti_tecnici, confidence):
    """
    Sizing v4 — Matrice ristretta (post-analisi log v3.2 ottobre 2026).
    Score 2/4 sempre rifiutato (fonte principale di falsi segnali storici).
    Conf max osservata in produzione = 8, quindi soglie tarate su 7-8.

    Score 4/4 + Conf >= 7  → 1.0 (setup ottimale)
    Score 3/4 + Conf >= 8  → 1.0 (setup forte)
    Score 3/4 + Conf == 7  → 0.5 (setup buono ma cauto)
    Tutto il resto         → 0.0 (no entry)
    """
    if punti_tecnici < 3 or confidence < 7:
        return 0.0
    if punti_tecnici == 4:
        return 1.0
    if punti_tecnici == 3 and confidence >= 8:
        return 1.0
    if punti_tecnici == 3 and confidence == 7:
        return 0.5
    return 0.0

# ─────────────────────────────────────────────────────────────────────────────
# 5. FAST MONITOR — 15s | Logica exit PRO-SPECIFICA
#    ATR trailing stop | Time-stop bypass se trailing attivo
# ─────────────────────────────────────────────────────────────────────────────
def fast_monitor_loop():
    global SESSION_PROFIT_PERC, SESSION_PROFIT_USD

    while True:
        time.sleep(15)
        ora_ny = datetime.datetime.now(pytz.timezone("America/New_York"))
        if (ora_ny.weekday() >= 5 or ora_ny.hour < 9
                or (ora_ny.hour == 9 and ora_ny.minute < 30) or ora_ny.hour >= 16):
            continue

        with state_lock:
            snap = {t: dict(d) for t, d in pos_attive.items()}

        for ticker, pos in snap.items():
            if pos.get('prezzo_carico') is None or pos.get('tp') is None:
                continue
            try:
                p_curr = yf.Ticker(ticker).history(period="1d", interval="1m", timeout=10)['Close'].iloc[-1]
                pc     = pos['prezzo_carico']
                pl     = ((p_curr - pc) / pc) * 100
                min_ap = (time.time() - pos['ts_apertura']) / 60
                atr_v  = pos.get('atr_val', pc * 0.005)

                # Break-even a +0.40%
                if pl >= 0.40 and pos.get('sl', 0) < (pc * 1.0005):
                    with state_lock:
                        if ticker in pos_attive:
                            pos_attive[ticker]['sl']              = pc * 1.0005
                            pos_attive[ticker]['trailing_attivo'] = True
                            stato.salva_stato(pos_attive)
                    comunicazioni.invia_telegram(f"🛡️ <b>BE ({ticker})</b>")

                # ATR trailing stop dinamico (solo verso l'alto)
                if pos.get('trailing_attivo') and p_curr > pc * 1.0005:
                    new_trail = p_curr - (1.0 * atr_v)
                    with state_lock:
                        if ticker in pos_attive and new_trail > pos_attive[ticker].get('sl', 0):
                            old_sl = round(pos_attive[ticker]['sl'], 2)
                            pos_attive[ticker]['sl'] = new_trail
                            stato.salva_stato(pos_attive)
                            print(f"📈 Trail {ticker}: SL {old_sl}$ → {round(new_trail,2)}$")

                # Leggo sl/tp aggiornati
                with state_lock:
                    pos_live = pos_attive.get(ticker, {})
                sl_live = pos_live.get('sl', pos.get('sl', pc * 0.995))
                tp_live = pos_live.get('tp', pos.get('tp', pc * 1.012))

                trailing_protetto = pos_live.get('trailing_attivo') and pl >= 0.30

                motivo = None
                if   p_curr <= sl_live:                         motivo = "Stop Loss [FastExit]" if not pos_live.get('trailing_attivo') else "Trailing Stop [FastExit]"
                elif p_curr >= tp_live:                         motivo = "Take Profit [FastExit]"
                elif min_ap >= 45 and not trailing_protetto:    motivo = "Time-Stop 45m [FastExit]"

                if motivo:
                    ok = etoro_engine.chiudi_e_logga_fast(
                        ticker, pos_live, p_curr, pl, motivo, pos_attive,
                        comunicazioni.invia_log_sheets_async, "PRO", cooldown_titoli
                    )
                    if ok:
                        SESSION_PROFIT_PERC += pl
                        SESSION_PROFIT_USD  += pos.get('importo_reale', IMPORTO_TEST) * (pl / 100.0)

            except Exception as e:
                print(f"Errore FastExit PRO {ticker}: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# 6. CORE LOOP — Strategia PRO: AI-driven Groq | MACD cross | Sizing matrix
# ─────────────────────────────────────────────────────────────────────────────
def monitora_mercato():
    global ULTIMA_SCANSIONE, cooldown_titoli, blocco_acquisti_fino_a, pos_attive
    global SESSION_PROFIT_PERC, SESSION_PROFIT_USD, CB_NOTIFICATO, LAST_SESSION_DATE
    global _ts_riconciliazione

    # Avvia il ping server SUBITO — prima di qualsiasi chiamata di rete
    # così Render rileva la porta e non fa timeout durante l'init
    stato.avvia_ping_server(int(os.environ.get("PORT", 10001)), "PRO")

    # Verifica chiavi eToro PRIMA di tutto
    if not etoro_engine.verifica_chiavi_avvio("PRO"):
        print("⛔ Avvio bloccato: chiavi eToro non valide. Correggi e rideploya.")
        return

    with state_lock:
        pos_attive.update(stato.carica_stato())

    def _panic_close_all(motivo_label):
        global SESSION_PROFIT_PERC, SESSION_PROFIT_USD
        with state_lock:
            snap = list(pos_attive.items())
        for t, p_dict in snap:
            ok, pl, imp = etoro_engine.chiudi_e_logga(
                t, p_dict, motivo_label, pos_attive,
                comunicazioni.invia_log_sheets_async, "PRO"
            )
            if ok:
                SESSION_PROFIT_PERC += pl
                SESSION_PROFIT_USD  += imp * (pl / 100.0)

    # Thread EOD dedicato: gira ogni 30s indipendente dal main loop
    # Garantisce panic close anche se il main loop è bloccato/morto
    def _eod_watchdog():
        ultimo_eod_data = None
        while True:
            try:
                time.sleep(30)
                ora_ny_w = datetime.datetime.now(pytz.timezone("America/New_York"))
                if ora_ny_w.weekday() >= 5:
                    continue
                if ultimo_eod_data != ora_ny_w.date() and ora_ny_w.hour < 15:
                    ultimo_eod_data = None
                if (ora_ny_w.hour == 15 and ora_ny_w.minute >= 44) or ora_ny_w.hour >= 16:
                    if ultimo_eod_data != ora_ny_w.date():
                        with state_lock:
                            ha_pos = bool(pos_attive)
                        if ha_pos:
                            comunicazioni.invia_telegram("🕒 <b>EOD WATCHDOG PRO</b>\nChiusura forzata posizioni residue.")
                            _panic_close_all("EOD Watchdog")
                        ultimo_eod_data = ora_ny_w.date()
            except Exception as e:
                print(f"⚠️ EOD watchdog PRO: {e}")

    Thread(target=fast_monitor_loop, daemon=True).start()
    Thread(target=_eod_watchdog, daemon=True).start()

    def _genera_status():
        """Dashboard con P/L live: fetch prezzi correnti per le posizioni aperte."""
        with state_lock:
            tickers_aperti = {t: d for t, d in pos_attive.items()
                              if (d.get('prezzo_carico') or d.get('prezzo'))}
        prezzi_live = {}
        for t in tickers_aperti:
            try:
                prezzi_live[t] = yf.Ticker(t).history(period="1d", interval="1m", timeout=8)['Close'].iloc[-1]
            except Exception:
                pass
        return comunicazioni.genera_dashboard_status(
            "RADAR PRO", "v3.9", pos_attive, state_lock,
            SESSION_PROFIT_PERC, SESSION_PROFIT_USD, START_TIME,
            BOT_PAUSA_MANUALE, ULTIMA_SCANSIONE,
            cb_attivo=(SESSION_PROFIT_PERC < DAILY_LOSS_LIMIT),
            prezzi_live=prezzi_live
        )

    comunicazioni.avvia_gestore_comandi(
        on_status = lambda: _genera_status(),
        on_pausa  = lambda: globals().__setitem__('BOT_PAUSA_MANUALE', True),
        on_avvia  = lambda: globals().__setitem__('BOT_PAUSA_MANUALE', False),
        on_chiudi = lambda: (_panic_close_all("Chiusura Manuale Chat"),
                             comunicazioni.invia_telegram("✅ Svuotamento completato."))
    )
    comunicazioni.invia_telegram(
        "🟢 <b>RADAR PRO v3.9 ONLINE</b>\n"
        "Alfa | SL ATR-based | Fix importo nominale"
    )

    while True:
        time.sleep((60 - (time.time() % 60)) + random.uniform(3, 8))
        try:
            ora_ny = datetime.datetime.now(pytz.timezone("America/New_York"))
            ULTIMA_SCANSIONE = ora_ny.strftime("%H:%M:%S")

            # Reset P/L giornaliero
            data_oggi = ora_ny.date()
            if LAST_SESSION_DATE != data_oggi:
                SESSION_PROFIT_PERC = 0.0
                SESSION_PROFIT_USD  = 0.0
                CB_NOTIFICATO       = False
                LAST_SESSION_DATE   = data_oggi

            if (ora_ny.weekday() >= 5 or ora_ny.hour < 9
                    or (ora_ny.hour == 9 and ora_ny.minute < 30) or ora_ny.hour >= 16):
                continue

            # Panic close EOD
            if ora_ny.hour == 15 and ora_ny.minute >= 45:
                with state_lock:
                    if pos_attive:
                        comunicazioni.invia_telegram("🕒 <b>PANIC CLOSE PRO (EOD)</b>")
                        _panic_close_all("Panic Close EOD")
                continue

            # Pausa pranzo (solo nuovi ingressi)
            ora_min    = ora_ny.hour * 100 + ora_ny.minute
            pausa_pranzo = 1130 <= ora_min < 1300

            # QQQ Guard
            is_breakout, blocco_acquisti_fino_a = qqq_guard.aggiorna_qqq(
                ora_ny, blocco_acquisti_fino_a, comunicazioni.invia_telegram, "PRO"
            )
            veto = qqq_guard.is_veto_attivo(blocco_acquisti_fino_a, ora_ny)
            with state_lock:
                max_p   = 5 if (is_breakout and not veto) else 4
                len_pos = len(pos_attive)

            # Circuit breaker
            cb_attivo, CB_NOTIFICATO = comunicazioni.check_circuit_breaker(
                SESSION_PROFIT_PERC, CB_NOTIFICATO, "PRO"
            )

            # Riconciliazione periodica posizioni con eToro (ogni 30 min)
            if pos_attive and (time.time() - _ts_riconciliazione) >= RICONCILIAZIONE_SEC:
                with state_lock:
                    pos_attive = etoro_engine.riconcilia_posizioni(pos_attive)
                _ts_riconciliazione = time.time()

            # Heartbeat log: stato sintetico ogni ciclo
            print(f"💓 [HEARTBEAT PRO] {ULTIMA_SCANSIONE} | QQQ_bull: {is_breakout} | Pos: {len_pos}/{max_p} | P/L: {round(SESSION_PROFIT_PERC,2)}% (${round(SESSION_PROFIT_USD,2)})")

            print(f"\n--- {ULTIMA_SCANSIONE} | Veto: {veto} | CB: {cb_attivo} | Pausa: {pausa_pranzo} | Pos: {len_pos}/{max_p}")

            if not pausa_pranzo:
                # Batch download 1m: scarica tutti i ticker in una sola chiamata
                try:
                    batch_1m = yf.download(
                        TICKERS, period="1d", interval="1m",
                        group_by="ticker", auto_adjust=True, timeout=20
                    )
                except Exception as e_batch:
                    print(f"⚠️ Batch download 1m PRO fallito: {e_batch} — skip ciclo")
                    batch_1m = None

                for ticker in TICKERS:
                    try:
                        with state_lock:
                            if (ticker in cooldown_titoli and ora_ny < cooldown_titoli[ticker]) \
                                    or ticker in pos_attive:
                                continue

                        if batch_1m is None:
                            continue

                        # Estrai dataframe del ticker dal batch
                        try:
                            if len(TICKERS) == 1:
                                df = batch_1m.copy()
                            else:
                                df = batch_1m[ticker].copy()
                            df = df.dropna(how='all')
                        except Exception:
                            continue
                        if df.empty or len(df) < 30:
                            continue

                        p = df['Close'].iloc[-1]

                        # Indicatori
                        df['RSI']      = indicatori.calcola_rsi(df)
                        df['VWAP']     = indicatori.calcola_vwap(df)
                        df['SMA20_Vol']= indicatori.calcola_vol_medio(df)
                        df['ATR']      = indicatori.calcola_atr(df)
                        macd, sig      = indicatori.calcola_macd(df)
                        df['MACD']     = macd
                        df['Signal']   = sig

                        rsi       = df['RSI'].iloc[-2]
                        vwap_val  = df['VWAP'].iloc[-1]
                        vol_ratio = df['Volume'].iloc[-2] / df['SMA20_Vol'].iloc[-2]
                        macd_bull = df['MACD'].iloc[-2] > df['Signal'].iloc[-2]
                        macd_cross = macd_bull and (df['MACD'].iloc[-3] <= df['Signal'].iloc[-3])
                        atr       = df['ATR'].iloc[-2]
                        atr_perc  = (atr / p) * 100

                        punti_tecnici = 0
                        if is_breakout:                                          punti_tecnici += 1
                        if p > vwap_val:                                         punti_tecnici += 1
                        if df['Volume'].iloc[-2] > df['SMA20_Vol'].iloc[-2] * 2: punti_tecnici += 1
                        h = ora_ny.hour
                        if 9 <= h < 10 or (h == 10 and ora_ny.minute <= 30):
                            if p > df['High'].iloc[:-1].max():                   punti_tecnici += 1
                        elif (h == 10 and ora_ny.minute > 30) or 11 <= h < 16:
                            if rsi <= (50 if p > vwap_val else 35):              punti_tecnici += 1
                        else:
                            if macd_cross:                                        punti_tecnici += 1

                        with state_lock:
                            len_pos = len(pos_attive)

                        puo_comprare = (len_pos < max_p and not veto
                                        and (ora_ny.hour < 15 or (ora_ny.hour == 15 and ora_ny.minute < 15))
                                        and not BOT_PAUSA_MANUALE and not cb_attivo)

                        size_mult  = 0.0
                        motivo_log = f"Score: {punti_tecnici}/4"

                        if puo_comprare and punti_tecnici >= 3:
                            e_groq, conf = interroga_groq(
                                ticker, p, rsi, vwap_val, vol_ratio, macd_bull, atr_perc, punti_tecnici
                            )
                            str_q      = "BUY" if e_groq is True else ("SKIP" if e_groq is False else "ERR")
                            motivo_log = f"Score: {punti_tecnici}/4 | Groq: {str_q} (Conf: {conf})"
                            if e_groq is True:
                                size_mult = calcola_size_v3(punti_tecnici, conf)

                        icona = "🔥" if (size_mult > 0 and not puo_comprare) else ("🚀" if size_mult > 0 else "🌙")
                        print(f"{icona} {ticker:4} | P: {round(p,2)}$ | Score: {punti_tecnici}/4 | Size: {size_mult}")

                        if puo_comprare and size_mult > 0:
                            # [FIX ACQUISTI DOPPI]
                            # execution_lock garantisce atomicità: un solo ticker
                            # alla volta può aprire. La sync è dentro il lock per
                            # evitare che il ciclo successivo veda uno stato stale.
                            with execution_lock:
                                with state_lock:
                                    # Doppio controllo: ticker potrebbe essere stato
                                    # aperto dal fast loop o da un ciclo sovrapposto
                                    if ticker in pos_attive:
                                        print(f"⚠️ SKIP {ticker}: già in portafoglio (doppio check)")
                                        continue
                                    if len(pos_attive) >= max_p:
                                        print(f"⚠️ SKIP {ticker}: limite {max_p} raggiunto")
                                        continue

                                nid = etoro_engine.apri_posizione(ticker, size_multiplier=size_mult, prezzo_corrente=p)
                                if nid:
                                    comunicazioni.invia_telegram(
                                        f"🚀 <b>{ticker} ACQ. ({motivo_log} | Sz: {size_mult})</b>"
                                    )
                                    with state_lock:
                                        sl_iniziale = max(p - (1.5 * atr), p * 0.995)
                                        pos_attive[ticker] = {
                                            "id":               nid,
                                            "prezzo_carico":    None,
                                            "prezzo_yahoo":     p,
                                            "atr_val":          atr,
                                            "ts_apertura":      time.time(),
                                            "ora_apertura_str": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            "sl":               sl_iniziale,
                                            "tp":               None,
                                            "trailing_attivo":  False,
                                            "importo_reale":    IMPORTO_TEST * size_mult,
                                        }
                                    stato.salva_stato(pos_attive)
                                    # Sync dentro il lock: aggiorna prezzo_carico reale
                                    # senza rischio di sovrascrivere altri ticker appena aperti
                                    time.sleep(1)
                                    with state_lock:
                                        pos_attive = etoro_engine.sincronizza_portafoglio(pos_attive)

                    except Exception:
                        print(f"Errore Scansione {ticker}:\n{traceback.format_exc()}")

            # Gestione portafoglio: imposta TP/SL ATR (attiva anche durante pausa pranzo)
            with state_lock:
                tkrs = list(pos_attive.keys())
            for t in tkrs:
                with state_lock:
                    if t not in pos_attive or pos_attive[t].get('prezzo_carico') is None:
                        continue
                    pos = pos_attive[t]
                pc = pos['prezzo_carico']
                if pos.get('tp') is None:
                    atr_v      = pos.get('atr_val', pc * 0.005)
                    sl_calc    = pc - (1.5 * atr_v)
                    tp_calc    = pc + (3.0 * atr_v)
                    if sl_calc > pc * 0.997: sl_calc, tp_calc = pc * 0.997, pc * 1.006
                    pos['sl'], pos['tp'] = sl_calc, tp_calc
                    stato.salva_stato(pos_attive)
                    comunicazioni.invia_telegram(
                        f"🎯 <b>{t} Target ATR</b>\nSL: {round(sl_calc,2)}$ | TP: {round(tp_calc,2)}$"
                    )

            with state_lock:
                pos_attive = etoro_engine.sincronizza_portafoglio(pos_attive)

        except Exception:
            print(f"Errore Loop PRO:\n{traceback.format_exc()}")

if __name__ == "__main__":
    monitora_mercato()
