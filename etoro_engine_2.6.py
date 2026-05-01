import uuid
import time
import datetime
import requests
import pytz
import yfinance as yf
from threading import Lock

_PUBLIC_KEY      = ""
_PRIVATE_KEY     = ""
_STRUMENTI       = {}
_MAPPA_ID        = {}
_IMPORTO_DEFAULT = 1000.0
_STATE_LOCK      = None
_INVIA_TELEGRAM  = None
_SALVA_STATO     = None
_COOLDOWN_MIN    = 10

# Cache equity portafoglio — evita chiamate API ridondanti tra cicli ravvicinati
_EQUITY_CACHE     = {"value": None, "timestamp": 0}
_EQUITY_CACHE_TTL = 60  # secondi

# Set condiviso: previene doppia chiusura da thread concorrenti (pattern Scalper)
chiusure_in_corso = set()

def init(public_key, private_key, strumenti, mappa_id, importo_default,
         state_lock, invia_telegram_fn, salva_stato_fn, cooldown_min=10):
    global _PUBLIC_KEY, _PRIVATE_KEY, _STRUMENTI, _MAPPA_ID
    global _IMPORTO_DEFAULT, _STATE_LOCK, _INVIA_TELEGRAM, _SALVA_STATO, _COOLDOWN_MIN
    _PUBLIC_KEY      = public_key
    _PRIVATE_KEY     = private_key
    _STRUMENTI       = strumenti
    _MAPPA_ID        = mappa_id
    _IMPORTO_DEFAULT = importo_default
    _STATE_LOCK      = state_lock
    _INVIA_TELEGRAM  = invia_telegram_fn
    _SALVA_STATO     = salva_stato_fn
    _COOLDOWN_MIN    = cooldown_min

def _headers():
    return {
        "x-api-key":    _PUBLIC_KEY,
        "x-user-key":   _PRIVATE_KEY,
        "x-request-id": str(uuid.uuid4()),
        "Content-Type": "application/json",
    }

# ---------------------------------------------------------------------------
# Verifica chiavi API all'avvio del bot
# ---------------------------------------------------------------------------
def verifica_chiavi_avvio(bot_name="BOT"):
    if not _PUBLIC_KEY or not _PRIVATE_KEY:
        print(f"❌ [{bot_name}] CHIAVI MANCANTI: ETORO_PUBLIC_KEY o ETORO_PRIVATE_KEY non configurate.")
        if _INVIA_TELEGRAM:
            _INVIA_TELEGRAM(f"❌ <b>[{bot_name}] CHIAVI eToro MANCANTI</b>\nVerifica le env vars su Render.")
        return False

    try:
        r = requests.get(
            "https://public-api.etoro.com/api/v1/trading/info/portfolio",
            headers={"X-API-Key": _PUBLIC_KEY, "X-User-Key": _PRIVATE_KEY,
                     "X-Request-Id": str(uuid.uuid4())},
            timeout=10
        )
        if r.status_code == 200:
            portafoglio = r.json().get("clientPortfolio", {})
            num_pos     = len(portafoglio.get("positions", []))
            print(f"✅ [{bot_name}] CHIAVI eToro OK — Portafoglio raggiungibile ({num_pos} posizioni esistenti)")
            return True

        elif r.status_code == 401:
            err_msg = f"❌ [{bot_name}] CHIAVI eToro INVALIDE (401 Unauthorized)"
            print(err_msg)
            print(f"   Risposta: {r.text}")
            if _INVIA_TELEGRAM:
                _INVIA_TELEGRAM(f"❌ <b>[{bot_name}] CHIAVI eToro INVALIDE</b>\n401 Unauthorized — Verifica le chiavi su Render.")
            return False

        elif r.status_code == 422:
            err_msg = f"❌ [{bot_name}] CHIAVE eToro NON VALIDA (422)"
            print(err_msg)
            print(f"   Risposta: {r.text}")
            if _INVIA_TELEGRAM:
                _INVIA_TELEGRAM(f"❌ <b>[{bot_name}] CHIAVE eToro NON VALIDA</b>\n422 — Rigenera le chiavi.")
            return False

        else:
            print(f"⚠️ [{bot_name}] Status inatteso da eToro ({r.status_code}): {r.text}")
            if _INVIA_TELEGRAM:
                _INVIA_TELEGRAM(f"⚠️ <b>[{bot_name}] eToro Status {r.status_code}</b>\nVerifica connessione/chiavi.")
            return False

    except Exception as e:
        print(f"❌ [{bot_name}] Errore connessione eToro: {e}")
        if _INVIA_TELEGRAM:
            _INVIA_TELEGRAM(f"❌ <b>[{bot_name}] Errore connessione eToro</b>\n{e}")
        return False

# ---------------------------------------------------------------------------
# Lettura equity netto del portafoglio Agente IA (con cache 60s)
#
# Su portafogli Agente IA, eToro interpreta `amount` nelle aperture come
# percentuale del capitale totale. Inviando (target_usd / equity * 100)
# si ottiene un'allocazione deterministica vicina al target.
#
# Cerca ricorsivamente campi equity standard nella risposta /portfolio.
# Fallback: somma units×openRate di tutte le posizioni aperte.
# Restituisce None se la lettura fallisce — in quel caso apri_posizione
# ricade sul comportamento by-units legacy.
# ---------------------------------------------------------------------------
def _get_equity_portafoglio(force_refresh=False):
    now = time.time()
    if (not force_refresh
            and _EQUITY_CACHE["value"] is not None
            and (now - _EQUITY_CACHE["timestamp"]) < _EQUITY_CACHE_TTL):
        return _EQUITY_CACHE["value"]

    try:
        r = requests.get(
            "https://public-api.etoro.com/api/v1/trading/info/portfolio",
            headers={"X-API-Key": _PUBLIC_KEY, "X-User-Key": _PRIVATE_KEY,
                     "X-Request-Id": str(uuid.uuid4())},
            timeout=10
        )
        if r.status_code != 200:
            print(f"⚠️ _get_equity_portafoglio: HTTP {r.status_code}")
            return None

        data = r.json()

        # Tentativo 1: cerca ricorsivamente campi equity standard
        chiavi_equity = {"Equity", "equity", "NetValue", "netValue",
                         "TotalEquity", "totalEquity", "PortfolioValue",
                         "portfolioValue", "AccountValue", "accountValue"}

        def _trova_equity(obj, chiavi):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k in chiavi and isinstance(v, (int, float)) and v > 0:
                        return float(v)
                    if isinstance(v, (dict, list)):
                        res = _trova_equity(v, chiavi)
                        if res is not None:
                            return res
            elif isinstance(obj, list):
                for item in obj:
                    res = _trova_equity(item, chiavi)
                    if res is not None:
                        return res
            return None

        equity = _trova_equity(data, chiavi_equity)

        # Tentativo 2: fallback — somma units×openRate delle posizioni aperte
        if equity is None:
            client_pf  = data.get("clientPortfolio", {})
            credit     = client_pf.get("credit") or client_pf.get("Credit") or 0.0
            posizioni  = client_pf.get("positions", [])
            equity_calc = float(credit)
            for p in posizioni:
                units     = p.get("units") or p.get("lotCount") or 0
                open_rate = p.get("openRate") or 0
                equity_calc += float(units) * float(open_rate)
            if equity_calc > 0:
                equity = equity_calc
                print(f"ℹ️ Equity calcolato da fallback (credit+posizioni): ${equity:.2f}")

        if equity is None or equity <= 0:
            print("⚠️ _get_equity_portafoglio: equity non trovato nella risposta")
            return None

        _EQUITY_CACHE["value"]     = equity
        _EQUITY_CACHE["timestamp"] = now
        return equity

    except Exception as e:
        print(f"❌ _get_equity_portafoglio: {e}")
        return None

# ---------------------------------------------------------------------------
# Apertura posizione
#
# Strategia sizing (Agente IA):
#   1. Legge equity netto del portafoglio.
#   2. Calcola amount_pct = (target_usd / equity) * 100.
#   3. Invia amount_pct via /by-amount — su Agente IA eToro interpreta
#      il campo `amount` come % del capitale, quindi l'allocazione reale
#      risulta deterministica e vicina al target in dollari.
#
# Fallback (se equity non disponibile):
#   Tenta /by-units con prezzo_corrente, poi /by-amount nominale legacy.
# ---------------------------------------------------------------------------
def apri_posizione(ticker, importo=None, size_multiplier=1.0, prezzo_corrente=None):
    try:
        amount = float(importo if importo is not None else _IMPORTO_DEFAULT) * size_multiplier

        # Tentativo primario: sizing % basata su equity portafoglio
        equity = _get_equity_portafoglio()
        if equity is not None and equity > 0:
            amount_pct = round((amount / equity) * 100, 2)
            print(f"💰 [{ticker}] Sizing %: target ${round(amount,2)} su equity "
                  f"${round(equity,0)} = {amount_pct}%")
            try:
                r_pct = requests.post(
                    "https://public-api.etoro.com/api/v1/trading/execution/market-open-orders/by-amount",
                    json={"instrumentId": _STRUMENTI.get(ticker), "isBuy": True,
                          "amount": amount_pct, "leverage": 1},
                    headers=_headers(), timeout=10
                )
                if r_pct.status_code in [200, 201, 202]:
                    print(f"✅ [{ticker}] Apertura by-amount%: {amount_pct}% (~${round(amount,2)})")
                    return r_pct.json().get("positionID") or True
                print(f"⚠️ [{ticker}] by-amount% fallito {r_pct.status_code}: {r_pct.text} — fallback a by-units")
            except Exception as e_pct:
                print(f"⚠️ [{ticker}] by-amount% eccezione: {e_pct} — fallback a by-units")

        # Fallback 1: /by-units con prezzo corrente
        if prezzo_corrente is not None and prezzo_corrente > 0:
            units = round(amount / prezzo_corrente, 6)
            try:
                r_units = requests.post(
                    "https://public-api.etoro.com/api/v1/trading/execution/market-open-orders/by-units",
                    json={"InstrumentId": _STRUMENTI.get(ticker), "IsBuy": True,
                          "AmountInUnits": units, "Leverage": 1},
                    headers=_headers(), timeout=10
                )
                if r_units.status_code in [200, 201, 202]:
                    print(f"✅ [{ticker}] Apertura by-units: {units} unità (~${round(amount,2)})")
                    return r_units.json().get("positionID") or True
                print(f"⚠️ [{ticker}] by-units fallito {r_units.status_code}: {r_units.text} — fallback a by-amount nominale")
            except Exception as e_units:
                print(f"⚠️ [{ticker}] by-units eccezione: {e_units} — fallback a by-amount nominale")

        # Fallback 2: /by-amount nominale legacy
        r = requests.post(
            "https://public-api.etoro.com/api/v1/trading/execution/market-open-orders/by-amount",
            json={"instrumentId": _STRUMENTI.get(ticker), "isBuy": True, "amount": amount, "leverage": 1},
            headers=_headers(), timeout=10
        )
        if r.status_code in [200, 201, 202]:
            print(f"✅ [{ticker}] Apertura by-amount nominale: ${round(amount,2)} (fallback legacy)")
            return r.json().get("positionID") or True
        print(f"Errore eToro Apri [{ticker}] {r.status_code}: {r.text}")
    except Exception as e:
        print(f"Eccezione eToro Apri [{ticker}]: {e}")
    return None

# ---------------------------------------------------------------------------
# Chiusura posizione (base)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Recupero closeRate reale da trade history dopo chiusura
# eToro non restituisce il prezzo di esecuzione nella risposta POST close.
# Chiamiamo GET /trade/history con minDate = 2 minuti prima della chiusura
# e cerchiamo il trade per positionId. Retry x3 con backoff da 1s.
# ---------------------------------------------------------------------------
def _get_close_rate_from_history(pos_id, fallback_price, tentativi=3):
    import datetime as _dt
    min_date = (_dt.datetime.utcnow() - _dt.timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for i in range(tentativi):
        try:
            r = requests.get(
                "https://public-api.etoro.com/api/v1/trading/info/trade/history",
                params={"minDate": min_date},
                headers={"X-API-Key": _PUBLIC_KEY, "X-User-Key": _PRIVATE_KEY,
                         "X-Request-Id": str(uuid.uuid4())},
                timeout=10
            )
            if r.status_code == 200:
                trades = r.json()
                if isinstance(trades, list):
                    items = trades
                elif isinstance(trades, dict):
                    items = trades.get("trades", trades.get("data", []))
                else:
                    items = []
                for t in items:
                    if str(t.get("positionId", "")) == str(pos_id) or \
                       str(t.get("positionID", "")) == str(pos_id):
                        cr = t.get("closeRate") or t.get("CloseRate")
                        if cr and float(cr) > 0:
                            print(f"✅ closeRate reale da history [{pos_id}]: {cr}")
                            return float(cr)
        except Exception as e:
            print(f"⚠️ _get_close_rate_from_history tentativo {i+1}: {e}")
        if i < tentativi - 1:
            time.sleep(1 + i)
    print(f"⚠️ closeRate non trovato in history [{pos_id}] — uso fallback yf: {fallback_price}")
    return fallback_price


def chiudi_posizione(ticker, pos_id, prezzo_chiusura_yf, motivo, cooldown_titoli=None):
    if pos_id is True:
        print(f"⚠️ [{ticker}] Chiusura bloccata: positionID non ancora confermato (id=True).")
        return False, None
    try:
        r = requests.post(
            f"https://public-api.etoro.com/api/v1/trading/execution/market-close-orders/positions/{pos_id}",
            json={"instrumentId": _STRUMENTI.get(ticker), "unitsToDeduct": None},
            headers=_headers(), timeout=10
        )
        if r.status_code in [200, 201, 202]:
            resp_json = r.json()

            def _trova_prezzo(obj, chiavi_target):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k in chiavi_target and isinstance(v, (int, float)) and v > 0:
                            return float(v)
                        if isinstance(v, (dict, list)):
                            res = _trova_prezzo(v, chiavi_target)
                            if res is not None:
                                return res
                elif isinstance(obj, list):
                    for item in obj:
                        res = _trova_prezzo(item, chiavi_target)
                        if res is not None:
                            return res
                return None

            chiavi_prezzo = {"closeRate", "executionRate", "executedPrice", "rate",
                             "closingRate", "price", "currentRate"}
            close_rate = _trova_prezzo(resp_json, chiavi_prezzo)

            # closeRate non presente nella risposta POST — recupera da trade history
            if close_rate is None:
                close_rate = _get_close_rate_from_history(pos_id, prezzo_chiusura_yf)

            if _INVIA_TELEGRAM:
                _INVIA_TELEGRAM(
                    f"🏁 <b>{ticker} VEND. ({motivo})</b>\n"
                    f"eToro: {round(close_rate, 4)}$ | yf: {round(prezzo_chiusura_yf, 4)}$"
                )
            if cooldown_titoli is not None and ("Stop Loss" in motivo or "Time-Stop" in motivo or "Time-SL" in motivo):
                with _STATE_LOCK:
                    cooldown_titoli[ticker] = (
                        datetime.datetime.now(pytz.timezone("America/New_York"))
                        + datetime.timedelta(minutes=_COOLDOWN_MIN)
                    )
            return True, close_rate
        print(f"Errore eToro Chiudi [{ticker}] {r.status_code}: {r.text}")
    except Exception as e:
        print(f"Eccezione eToro Chiudi [{ticker}]: {e}")
    return False, None

# ---------------------------------------------------------------------------
# Chiusura con retry e backoff esponenziale
# ---------------------------------------------------------------------------
def chiudi_posizione_con_retry(ticker, pos_id, prezzo_chiusura_yf, motivo,
                                cooldown_titoli=None, tentativi=3):
    for i in range(tentativi):
        ok, close_rate = chiudi_posizione(ticker, pos_id, prezzo_chiusura_yf, motivo, cooldown_titoli)
        if ok:
            return True, close_rate
        wait = 2 ** i
        print(f"⚠️ Retry chiusura {ticker} ({i+1}/{tentativi}) tra {wait}s...")
        if i < tentativi - 1:
            time.sleep(wait)
    if _INVIA_TELEGRAM:
        _INVIA_TELEGRAM(f"🚨 <b>FALLITA chiusura {ticker}</b> dopo {tentativi} tentativi!\nIntervento manuale richiesto su eToro.")
    return False, None

# ---------------------------------------------------------------------------
# Lettura importo reale da eToro per positionID
#
# FIX IMPORTO: questa funzione viene chiamata al momento della chiusura per
# ottenere il valore reale allocato da eToro (che per i portafogli Agente IA
# differisce dall'importo nominale inviato, in quanto eToro lo converte in
# percentuale del capitale totale del portafoglio).
#
# Ordine di priorità campi:
#   initialMargin → buyAmount → amount
# Se nessuno è > 0, ritorna None.
# ---------------------------------------------------------------------------
def _leggi_importo_reale_da_etoro(pos_id):
    try:
        r = requests.get(
            "https://public-api.etoro.com/api/v1/trading/info/portfolio",
            headers={"X-API-Key": _PUBLIC_KEY, "X-User-Key": _PRIVATE_KEY,
                     "X-Request-Id": str(uuid.uuid4())},
            timeout=8
        )
        if r.status_code == 200:
            for p in r.json().get("clientPortfolio", {}).get("positions", []):
                if str(p.get("positionID")) == str(pos_id):
                    units     = p.get('units') or p.get('lotCount') or 0
                    open_rate = p.get('openRate') or 0
                    if units > 0 and open_rate > 0:
                        return round(units * open_rate, 2)
                    return None
    except Exception as e:
        print(f"⚠️ _leggi_importo_reale_da_etoro: {e}")
    return None

# ---------------------------------------------------------------------------
# _risolvi_importo_reale — logica unificata per chiudi_e_logga e _fast
#
# Strategia:
#   1. Legge l'importo_reale già nello stato (aggiornato dalla sync).
#   2. Tenta sempre una lettura diretta da eToro al momento della chiusura
#      (la posizione esiste ancora in questo momento).
#   3. Se il valore eToro è disponibile E differisce di più del 5% da quello
#      in stato, lo preferisce (Agente IA rimappa l'importo nominale).
#   4. Logga sempre il delta per tracciabilità.
# ---------------------------------------------------------------------------
def _risolvi_importo_reale(ticker, pos_snapshot):
    importo_stato = pos_snapshot.get('importo_reale', _IMPORTO_DEFAULT)
    pos_id        = pos_snapshot.get('id')

    importo_etoro = _leggi_importo_reale_da_etoro(pos_id) if pos_id and pos_id is not True else None

    if importo_etoro and importo_etoro > 0:
        delta_perc = abs(importo_etoro - importo_stato) / max(importo_stato, 1) * 100
        if delta_perc > 5:
            print(f"💰 [{ticker}] importo_reale corretto: ${round(importo_etoro, 2)} "
                  f"(stato aveva ${round(importo_stato, 2)}, delta {round(delta_perc, 1)}%)")
            return importo_etoro
        else:
            # Valori coerenti — usa quello già in stato (sync già aveva il dato giusto)
            print(f"✅ [{ticker}] importo_reale confermato: ${round(importo_stato, 2)} "
                  f"(eToro: ${round(importo_etoro, 2)}, delta {round(delta_perc, 1)}%)")
            return importo_stato

    # Fallback: eToro non ha restituito nulla (posizione già chiusa lato API)
    print(f"⚠️ [{ticker}] _leggi_importo_reale ha restituito None — uso importo da stato: ${round(importo_stato, 2)}")
    return importo_stato

# ---------------------------------------------------------------------------
# chiudi_e_logga — pattern unificato
# ---------------------------------------------------------------------------
def chiudi_e_logga(ticker, pos_snapshot, motivo, pos_attive, invia_log_fn,
                   bot_name, cooldown_titoli=None):
    with _STATE_LOCK:
        if ticker in chiusure_in_corso or ticker not in pos_attive:
            return False, 0.0, 0.0
        chiusure_in_corso.add(ticker)

    try:
        pc = pos_snapshot.get('prezzo_carico') or pos_snapshot.get('prezzo', 0) or 0

        try:
            p_yf = yf.Ticker(ticker).history(period="1d", interval="1m", timeout=10)['Close'].iloc[-1]
        except Exception:
            p_yf = pc

        # FIX: legge l'importo reale da eToro PRIMA della chiusura (posizione ancora presente in portafoglio)
        importo = _risolvi_importo_reale(ticker, pos_snapshot)

        ok, close_rate = chiudi_posizione_con_retry(ticker, pos_snapshot['id'], p_yf, motivo, cooldown_titoli)
        if not ok:
            return False, 0.0, 0.0

        p_etoro = close_rate if close_rate else p_yf
        pl      = ((p_etoro - pc) / pc) * 100 if pc else 0.0

        invia_log_fn(
            bot_name, ticker, "LONG",
            pos_snapshot.get('ora_apertura_str', 'N/A'),
            pos_snapshot.get('prezzo_yahoo', pc),
            pc, p_etoro, motivo, pl, importo,
            prezzo_chiusura_etoro=p_etoro,
            prezzo_chiusura_yf=p_yf
        )

        with _STATE_LOCK:
            pos_attive.pop(ticker, None)
        if _SALVA_STATO:
            _SALVA_STATO(pos_attive)

        return True, pl, importo

    except Exception as e:
        print(f"Errore chiudi_e_logga [{ticker}]: {e}")
        return False, 0.0, 0.0
    finally:
        with _STATE_LOCK:
            chiusure_in_corso.discard(ticker)

# ---------------------------------------------------------------------------
# chiudi_e_logga_fast — variante con prezzo e P/L già calcolati dal fast monitor
# ---------------------------------------------------------------------------
def chiudi_e_logga_fast(ticker, pos_snapshot, p_yf, pl_perc_yf, motivo,
                         pos_attive, invia_log_fn, bot_name, cooldown_titoli=None):
    with _STATE_LOCK:
        if ticker in chiusure_in_corso or ticker not in pos_attive:
            return False
        chiusure_in_corso.add(ticker)

    try:
        pc = pos_snapshot.get('prezzo_carico') or pos_snapshot.get('prezzo', p_yf) or p_yf

        # FIX: legge l'importo reale da eToro PRIMA della chiusura (posizione ancora presente in portafoglio)
        importo = _risolvi_importo_reale(ticker, pos_snapshot)

        ok, close_rate = chiudi_posizione_con_retry(ticker, pos_snapshot['id'], p_yf, motivo, cooldown_titoli)
        if not ok:
            return False

        p_etoro  = close_rate if close_rate else p_yf
        pl_reale = ((p_etoro - pc) / pc) * 100 if pc else pl_perc_yf

        invia_log_fn(
            bot_name, ticker, "LONG",
            pos_snapshot.get('ora_apertura_str', 'N/A'),
            pos_snapshot.get('prezzo_yahoo', pc),
            pc, p_etoro, motivo, pl_reale, importo,
            prezzo_chiusura_etoro=p_etoro,
            prezzo_chiusura_yf=p_yf
        )

        with _STATE_LOCK:
            pos_attive.pop(ticker, None)
        if _SALVA_STATO:
            _SALVA_STATO(pos_attive)

        return True

    except Exception as e:
        print(f"Errore chiudi_e_logga_fast [{ticker}]: {e}")
        return False
    finally:
        with _STATE_LOCK:
            chiusure_in_corso.discard(ticker)

# ---------------------------------------------------------------------------
# Chiusura parziale 50% via UnitsToDeduct (Partial TP reale)
# Chiude metà posizione, aggiorna stato con importo dimezzato e SL a BE.
# Logga il parziale su Sheets come trade separato.
# Restituisce True se la chiusura parziale ha avuto successo.
# ---------------------------------------------------------------------------
def chiudi_parziale_50(ticker, pos_snapshot, p_yf, pos_attive, invia_log_fn, bot_name):
    with _STATE_LOCK:
        if ticker in chiusure_in_corso or ticker not in pos_attive:
            return False
        # non blocca chiusure_in_corso: la posizione rimane aperta per il 50% residuo

    pos_id = pos_snapshot.get('id')
    if not pos_id or pos_id is True:
        print(f"⚠️ [{ticker}] Partial TP bloccato: positionID non valido.")
        return False

    pc        = pos_snapshot.get('prezzo_carico') or pos_snapshot.get('prezzo', p_yf) or p_yf
    importo   = pos_snapshot.get('importo_reale', _IMPORTO_DEFAULT)
    importo50 = round(importo / 2, 2)

    # Calcola unità della posizione: importo / prezzo_carico
    if pc > 0:
        units_totali = importo / pc
        units_50     = round(units_totali / 2, 6)
    else:
        print(f"⚠️ [{ticker}] Partial TP: prezzo_carico non disponibile.")
        return False

    try:
        r = requests.post(
            f"https://public-api.etoro.com/api/v1/trading/execution/market-close-orders/positions/{pos_id}",
            json={"InstrumentId": _STRUMENTI.get(ticker), "UnitsToDeduct": units_50},
            headers=_headers(), timeout=10
        )
        if r.status_code not in [200, 201, 202]:
            print(f"⚠️ [{ticker}] Partial TP fallito {r.status_code}: {r.text}")
            return False

        # Recupera closeRate reale
        close_rate = _get_close_rate_from_history(pos_id, p_yf)
        pl_reale   = ((close_rate - pc) / pc) * 100 if pc else 0.0

        # Logga il 50% chiuso su Sheets
        invia_log_fn(
            bot_name, ticker, "LONG",
            pos_snapshot.get('ora_apertura_str', 'N/A'),
            pos_snapshot.get('prezzo_yahoo', pc),
            pc, close_rate, "Partial TP 50% ATR×2", pl_reale, importo50,
            prezzo_chiusura_etoro=close_rate,
            prezzo_chiusura_yf=p_yf
        )

        # Aggiorna stato: importo dimezzato, SL a break-even, trailing attivo
        with _STATE_LOCK:
            if ticker in pos_attive:
                pos_attive[ticker]['importo_reale']    = importo50
                pos_attive[ticker]['partial_tp_eseguito'] = True
                pos_attive[ticker]['trailing_attivo']  = True
                # SL a break-even per proteggere il 50% residuo
                be_sl = pc * 1.0005
                if be_sl > pos_attive[ticker].get('sl', 0):
                    pos_attive[ticker]['sl'] = be_sl
        if _SALVA_STATO:
            _SALVA_STATO(pos_attive)

        if _INVIA_TELEGRAM:
            _INVIA_TELEGRAM(
                f"✂️ <b>PARTIAL TP 50% ({ticker})</b>\n"
                f"Chiuse {round(units_50, 4)} unità a {round(close_rate, 4)}$\n"
                f"P/L parziale: {round(pl_reale, 2)}% | ${round(importo50 * (pl_reale/100), 2)}\n"
                f"🛡️ SL residuo → BE ({round(pc * 1.0005, 2)}$)"
            )

        print(f"✅ [{ticker}] Partial TP 50%: {round(units_50, 4)} unità chiuse, residuo ${round(importo50, 2)}")
        return True

    except Exception as e:
        print(f"Errore chiudi_parziale_50 [{ticker}]: {e}")
        return False

# ---------------------------------------------------------------------------
# Sincronizzazione portafoglio da eToro
# ---------------------------------------------------------------------------
def sincronizza_portafoglio(pos_attive_locali):
    try:
        r = requests.get(
            "https://public-api.etoro.com/api/v1/trading/info/portfolio",
            headers={"X-API-Key": _PUBLIC_KEY, "X-User-Key": _PRIVATE_KEY,
                     "X-Request-Id": str(uuid.uuid4())},
            timeout=10
        )
        if r.status_code == 200:
            ora_now = time.time()

            pos_remote_by_id     = {}
            pos_remote_by_ticker = {}
            for p in r.json().get("clientPortfolio", {}).get("positions", []):
                if p["instrumentID"] not in _MAPPA_ID:
                    continue
                t = _MAPPA_ID[p["instrumentID"]]
                pos_remote_by_id[str(p["positionID"])]  = p
                pos_remote_by_ticker[t]                  = p

            pos_aggiornate = {}

            for t, d in pos_attive_locali.items():
                pos_id = d.get('id')

                # Caso 1: posizione ghost (id=True)
                if pos_id is True:
                    if 'ts_apertura' in d and (ora_now - d['ts_apertura']) < 300:
                        p_reale = pos_remote_by_ticker.get(t)
                        if p_reale:
                            units     = p_reale.get('units') or p_reale.get('lotCount') or 0
                            open_rate = p_reale.get('openRate') or 0
                            importo   = round(units * open_rate, 2) if (units > 0 and open_rate > 0) else _IMPORTO_DEFAULT
                            aggiornata = d.copy()
                            aggiornata['id']            = p_reale["positionID"]
                            aggiornata['prezzo_carico'] = p_reale["openRate"]
                            aggiornata['prezzo']        = p_reale["openRate"]
                            aggiornata['importo_reale'] = importo
                            pos_aggiornate[t] = aggiornata
                            print(f"✅ Ghost [{t}] risolta: positionID {p_reale['positionID']} "
                                  f"confermato da eToro.")
                        else:
                            pos_aggiornate[t] = d
                    else:
                        p_reale = pos_remote_by_ticker.get(t)
                        if p_reale:
                            print(f"🔍 Ghost [{t}]: trovata su eToro (posID: {p_reale['positionID']}). Tento chiusura...")
                            try:
                                r_close = requests.post(
                                    f"https://public-api.etoro.com/api/v1/trading/execution/"
                                    f"market-close-orders/positions/{p_reale['positionID']}",
                                    json={"instrumentId": _STRUMENTI.get(t), "unitsToDeduct": None},
                                    headers=_headers(), timeout=10
                                )
                                if r_close.status_code in [200, 201, 202]:
                                    print(f"✅ Ghost [{t}] chiusa su eToro.")
                                    if _INVIA_TELEGRAM:
                                        _INVIA_TELEGRAM(
                                            f"⚠️ <b>Ghost [{t}] chiusa automaticamente</b>\n"
                                            f"Posizione aperta senza conferma ID — chiusa al rilevamento."
                                        )
                                else:
                                    print(f"❌ Ghost [{t}]: chiusura fallita {r_close.status_code}: {r_close.text}")
                                    if _INVIA_TELEGRAM:
                                        _INVIA_TELEGRAM(
                                            f"🚨 <b>Ghost [{t}] NON chiusa</b>\n"
                                            f"Errore {r_close.status_code}\n"
                                            f"<b>Verifica e chiudi manualmente su eToro.</b>"
                                        )
                            except Exception as e_close:
                                print(f"❌ Ghost [{t}]: eccezione chiusura: {e_close}")
                                if _INVIA_TELEGRAM:
                                    _INVIA_TELEGRAM(
                                        f"🚨 <b>Ghost [{t}] NON chiusa</b>\n"
                                        f"{e_close}\n"
                                        f"<b>Verifica e chiudi manualmente su eToro.</b>"
                                    )
                        else:
                            print(f"⚠️ Ghost [{t}] rimossa: non trovata su eToro dopo 5m.")
                            if _INVIA_TELEGRAM:
                                _INVIA_TELEGRAM(
                                    f"⚠️ <b>Ghost [{t}] rimossa</b>\n"
                                    f"Non trovata su eToro dopo 5m — ordine probabilmente non eseguito."
                                )
                    continue

                # Caso 2: posizione nota — aggiorna prezzo carico e importo
                p_etoro = pos_remote_by_id.get(str(pos_id)) or pos_remote_by_ticker.get(t)
                if p_etoro:
                    units     = p_etoro.get('units') or p_etoro.get('lotCount') or 0
                    open_rate = p_etoro.get('openRate') or 0
                    importo   = round(units * open_rate, 2) if (units > 0 and open_rate > 0) else _IMPORTO_DEFAULT
                    aggiornata = d.copy()
                    aggiornata['id']            = p_etoro["positionID"]
                    aggiornata['prezzo_carico'] = p_etoro["openRate"]
                    aggiornata['prezzo']        = p_etoro["openRate"]
                    aggiornata['importo_reale'] = importo
                    pos_aggiornate[t] = aggiornata
                else:
                    pos_aggiornate[t] = d

            return pos_aggiornate

    except Exception as e:
        print(f"Eccezione eToro Sync: {e}")
    return pos_attive_locali

# ---------------------------------------------------------------------------
# Riconciliazione periodica posizioni: cerca ID reale per instrumentId
# Da chiamare periodicamente (es. ogni 30 min) dal main loop di ogni bot.
# Risolve il caso in cui pos_attive ha un id che non corrisponde più a una
# posizione eToro reale (es. id=True non risolto, ID corrotti dopo reboot).
# ---------------------------------------------------------------------------
def riconcilia_posizioni(pos_attive_locali):
    if not pos_attive_locali:
        return pos_attive_locali
    try:
        r = requests.get(
            "https://public-api.etoro.com/api/v1/trading/info/portfolio",
            headers={"X-API-Key": _PUBLIC_KEY, "X-User-Key": _PRIVATE_KEY,
                     "X-Request-Id": str(uuid.uuid4())},
            timeout=10
        )
        if r.status_code != 200:
            return pos_attive_locali

        positions = r.json().get("clientPortfolio", {}).get("positions", [])
        # Mappa: instrumentId -> lista posizioni reali per quello strumento
        pos_per_strumento = {}
        for p in positions:
            iid = p.get("instrumentID")
            if iid in _MAPPA_ID:
                pos_per_strumento.setdefault(iid, []).append(p)

        riconciliate = 0
        for t, d in pos_attive_locali.items():
            pos_id_locale = str(d.get('id', ''))
            iid_strumento = _STRUMENTI.get(t)
            if not iid_strumento or iid_strumento not in pos_per_strumento:
                continue
            # Cerca match per ID locale tra le posizioni reali per quello strumento
            candidati = pos_per_strumento[iid_strumento]
            id_reali  = [str(p.get("positionID")) for p in candidati]
            if pos_id_locale in id_reali:
                continue  # ID ancora valido, nessuna azione
            # ID non trovato: prendi la posizione più recente per quello strumento
            piu_recente = candidati[-1]
            id_nuovo    = piu_recente.get("positionID")
            if id_nuovo and str(id_nuovo) != pos_id_locale:
                d['id'] = id_nuovo
                riconciliate += 1
                print(f"🔄 Riconciliato [{t}]: id {pos_id_locale} → {id_nuovo}")

        if riconciliate > 0 and _INVIA_TELEGRAM:
            _INVIA_TELEGRAM(
                f"🔄 <b>Riconciliazione completata</b>\n"
                f"{riconciliate} posizioni con ID aggiornato da eToro."
            )
        return pos_attive_locali
    except Exception as e:
        print(f"⚠️ riconcilia_posizioni: {e}")
        return pos_attive_locali
