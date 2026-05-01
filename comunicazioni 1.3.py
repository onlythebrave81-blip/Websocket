import time
import datetime
import requests
import pytz
import json
from threading import Thread

_TOKEN      = ""
_CHAT_ID    = ""
_LOG_URL    = ""

def init(telegram_token, telegram_chat_id, log_script_url):
    global _TOKEN, _CHAT_ID, _LOG_URL
    _TOKEN   = telegram_token
    _CHAT_ID = telegram_chat_id
    _LOG_URL = log_script_url

# ---------------------------------------------------------------------------
# Tastiera inline standard — presente su ogni risposta /status
# ---------------------------------------------------------------------------
_TASTIERA_STANDARD = [[
    {"text": "▶️ AVVIA",  "callback_data": "/avvia"},
    {"text": "⏸️ PAUSA",  "callback_data": "/pausa"},
], [
    {"text": "📊 STATUS",       "callback_data": "/status"},
    {"text": "🚨 PANIC CLOSE",  "callback_data": "/chiudi"},
]]

# ---------------------------------------------------------------------------
# Telegram — supporto bottoni inline opzionali
# ---------------------------------------------------------------------------
def invia_telegram(msg, bottoni=None):
    try:
        payload = {
            "chat_id":    _CHAT_ID,
            "text":       msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if bottoni:
            payload["reply_markup"] = json.dumps({"inline_keyboard": bottoni})
        requests.post(
            f"https://api.telegram.org/bot{_TOKEN}/sendMessage",
            json=payload, timeout=5
        )
    except Exception as e:
        print(f"Errore Telegram: {e}")

def _rispondi_callback(callback_query_id):
    """Toglie il loader dal bottone inline dopo il tap."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id},
            timeout=5
        )
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Log Google Sheets (asincrono)
# ---------------------------------------------------------------------------
def invia_log_sheets_async(bot, ticker, direzione, ora_apertura, prezzo_yahoo,
                            prezzo_etoro, p_uscita, motivo, pl_perc, importo_reale=None,
                            prezzo_chiusura_etoro=None, prezzo_chiusura_yf=None):
    def task():
        importo    = importo_reale if importo_reale is not None else 0.0
        pl_dollari = importo * (pl_perc / 100.0) if pl_perc else 0.0

        spread_chiusura_perc = None
        if prezzo_chiusura_etoro and prezzo_chiusura_yf and prezzo_chiusura_etoro > 0:
            spread_chiusura_perc = round(
                ((prezzo_chiusura_yf - prezzo_chiusura_etoro) / prezzo_chiusura_etoro) * 100, 4
            )

        # trade_id univoco: BOT_TICKER_YYYYMMDD_HHMMSS (al momento della chiusura)
        now         = datetime.datetime.now()
        trade_id    = f"{bot}_{ticker}_{now.strftime('%Y%m%d_%H%M%S')}"
        data_chiusura = now.strftime("%Y-%m-%d %H:%M:%S")

        # durata_minuti: calcolata in Python, non come formula Sheets
        durata_minuti = None
        if ora_apertura:
            try:
                fmt = "%Y-%m-%d %H:%M:%S"
                t_apertura  = datetime.datetime.strptime(ora_apertura, fmt)
                t_chiusura  = datetime.datetime.strptime(data_chiusura, fmt)
                durata_minuti = round((t_chiusura - t_apertura).total_seconds() / 60, 1)
            except Exception:
                durata_minuti = None

        payload = {
            "trade_id":                trade_id,
            "ora_apertura":            ora_apertura              or "",
            "data_ora":                data_chiusura,
            "durata_minuti":           durata_minuti             if durata_minuti is not None else "",
            "bot":                     bot,
            "ticker":                  ticker,
            "direzione":               direzione,
            "prezzo_yahoo":            prezzo_yahoo              or "",
            "prezzo_etoro":            prezzo_etoro              or "",
            "p_ingresso":              prezzo_etoro              or "",
            "prezzo_chiusura_etoro":   round(prezzo_chiusura_etoro, 4) if prezzo_chiusura_etoro else "",
            "prezzo_chiusura_yf":      round(prezzo_chiusura_yf, 4)    if prezzo_chiusura_yf    else "",
            "p_uscita":                p_uscita                  or "",
            "spread_chiusura_perc":    spread_chiusura_perc      if spread_chiusura_perc is not None else "",
            "pl_perc":                 round(pl_perc, 2)         if pl_perc is not None else 0.0,
            "importo_investito":       round(importo, 2),
            "pl_dollari":              round(pl_dollari, 2),
            "importo_uscita":          round(importo + pl_dollari, 2),
            "motivo":                  motivo,
        }
        try:
            requests.post(_LOG_URL, json=payload, timeout=10)
        except Exception as e:
            print(f"Errore Log Sheets: {e}")
    Thread(target=task, daemon=True).start()

# ---------------------------------------------------------------------------
# Dashboard unificata con P/L live per posizione e bottoni inline
# ---------------------------------------------------------------------------
def genera_dashboard_status(bot_name, version, pos_attive, state_lock,
                             session_profit_perc, session_profit_usd,
                             start_time, bot_pausa_manuale, ultima_scansione,
                             max_pos=4, cb_attivo=False, cb_limit=-2.5,
                             qqq_dati_disponibili=None, prezzi_live=None):
    """
    prezzi_live: dict opzionale {ticker: float} con prezzi correnti.
    Se fornito, mostra P/L live per ogni posizione aperta.
    """
    uptime_sec = int(time.time() - start_time)
    ore, resto = divmod(uptime_sec, 3600)
    minuti, _  = divmod(resto, 60)
    ora_ny     = datetime.datetime.now(pytz.timezone("America/New_York"))
    mercato_ok = (9 <= ora_ny.hour < 16 and ora_ny.weekday() < 5
                  and not (ora_ny.hour == 9 and ora_ny.minute < 30))

    icona_merc = "🟢 APERTO"  if mercato_ok        else "🔴 CHIUSO"
    stato_bot  = "🟡 PAUSA"   if bot_pausa_manuale else "🟢 ATTIVO"
    cb_stato   = "🔴 ATTIVO"  if cb_attivo         else "🟢 OK"

    with state_lock:
        copia_pos = dict(pos_attive)

    sep = "━━━━━━━━━━━━━━━━━"

    lista_pos = ""
    pl_open_totale_usd = 0.0
    if not copia_pos:
        lista_pos = "  Nessuna posizione aperta\n"
    else:
        for t, d in copia_pos.items():
            pc      = d.get('prezzo_carico') or d.get('prezzo', 0) or 0
            tp      = d.get('tp', 0) or 0
            sl      = d.get('sl', 0) or 0
            trail   = "🎯" if d.get('trailing_attivo') else "  "
            importo = d.get('importo_reale', 0) or 0

            if pc > 0 and prezzi_live and t in prezzi_live:
                p_live = prezzi_live[t]
                pl_p   = ((p_live - pc) / pc) * 100
                pl_usd = importo * (pl_p / 100)
                pl_open_totale_usd += pl_usd
                emoji  = "✅" if pl_p >= 0 else "🔻"
                lista_pos += (
                    f"{trail} <b>{t}</b>: {round(p_live,2)}$ "
                    f"({emoji}{round(pl_p,2)}% | ${round(pl_usd,2)})\n"
                    f"     SL {round(sl,2)}$ → TP {round(tp,2)}$\n"
                )
            else:
                lista_pos += (
                    f"{trail} <b>{t}</b>: carico {round(pc,2)}$\n"
                    f"     SL {round(sl,2)}$ → TP {round(tp,2)}$\n"
                )

    msg  = f"🏛️ <b>{bot_name} {version}</b>\n{sep}\n"
    msg += f"📆 Mercato: {icona_merc}\n"
    msg += f"🤖 Stato: {stato_bot}\n"
    msg += f"🕒 Uptime: {ore}h {minuti}m | Check: {ultima_scansione}\n"
    if qqq_dati_disponibili is not None:
        msg += f"📊 QQQ Guard: {'✅ OK' if qqq_dati_disponibili else '⚠️ In attesa'}\n"
    msg += f"{sep}\n"
    msg += f"💼 <b>PORTAFOGLIO ({len(copia_pos)}/{max_pos})</b>\n{lista_pos}"
    if copia_pos and prezzi_live:
        emoji_tot = "✅" if pl_open_totale_usd >= 0 else "🔻"
        msg += f"  {emoji_tot} Open P/L live: <b>${round(pl_open_totale_usd, 2)}</b>\n"
    msg += f"{sep}\n"
    msg += f"📈 <b>OGGI:</b> {round(session_profit_perc, 2)}% | <b>${round(session_profit_usd, 2)}</b>\n"
    msg += f"🔒 CB: {cb_stato} (soglia {cb_limit}%)\n"
    return msg

# ---------------------------------------------------------------------------
# Circuit Breaker — soft block uniforme a -2.5%
# ---------------------------------------------------------------------------
CIRCUIT_BREAKER_LIMIT = -2.5

def check_circuit_breaker(session_profit_perc, cb_notificato,
                           bot_name="BOT", cb_limit=CIRCUIT_BREAKER_LIMIT):
    attivo = session_profit_perc < cb_limit
    if attivo and not cb_notificato:
        invia_telegram(
            f"🔒 <b>CIRCUIT BREAKER ATTIVATO ({bot_name})</b>\n"
            f"P/L: {round(session_profit_perc, 2)}% (soglia: {cb_limit}%)\n"
            f"Nessun nuovo ingresso. Posizioni aperte gestite normalmente."
        )
        return attivo, True
    return attivo, cb_notificato

# ---------------------------------------------------------------------------
# Gestore comandi Telegram con bottoni inline (callback_query)
# ---------------------------------------------------------------------------
def avvia_gestore_comandi(on_status, on_pausa, on_avvia, on_chiudi):
    def loop():
        last_update_id = 0
        while True:
            try:
                r = requests.get(
                    f"https://api.telegram.org/bot{_TOKEN}/getUpdates",
                    params={"offset": last_update_id + 1, "timeout": 30},
                    timeout=35
                )
                for u in r.json().get("result", []):
                    last_update_id = u["update_id"]
                    cmd = ""

                    # Bottoni inline → callback_query
                    if "callback_query" in u:
                        query = u["callback_query"]
                        chat_id_cb = str(query.get("message", {}).get("chat", {}).get("id", ""))
                        if chat_id_cb == _CHAT_ID:
                            cmd = query.get("data", "").lower()
                            _rispondi_callback(query["id"])

                    # Messaggi testuali
                    elif "message" in u:
                        if str(u["message"].get("chat", {}).get("id", "")) == _CHAT_ID:
                            cmd = u["message"].get("text", "").lower()

                    if not cmd:
                        continue

                    if "/status" in cmd:
                        invia_telegram(on_status(), bottoni=_TASTIERA_STANDARD)
                    elif "/pausa" in cmd:
                        on_pausa()
                        invia_telegram("⏸️ <b>Bot in PAUSA.</b>\nNuovi ingressi sospesi.", bottoni=_TASTIERA_STANDARD)
                    elif "/avvia" in cmd:
                        on_avvia()
                        invia_telegram("▶️ <b>Bot AVVIATO.</b>\nRicerca ingressi attiva.", bottoni=_TASTIERA_STANDARD)
                    elif "/chiudi" in cmd:
                        invia_telegram("🚨 <b>PANIC CLOSE!</b>\nChiusura in corso...", bottoni=_TASTIERA_STANDARD)
                        on_chiudi()

            except Exception:
                pass
            time.sleep(2)
    Thread(target=loop, daemon=True).start()
