import datetime
import pandas as pd
import yfinance as yf

def aggiorna_qqq(ora_ny, blocco_corrente, invia_telegram_fn=None, bot_name="BOT", veto_minuti=10):
    """
    Analizza QQQ e restituisce (is_breakout: bool, nuovo_blocco_fino_a: datetime|None).
    Se il veto viene attivato, invia notifica Telegram (se fn fornita).
    """
    try:
        qqq = yf.Ticker("QQQ").history(period="5d", interval="1m", timeout=10)
        if qqq.empty or len(qqq) < 15:
            return False, blocco_corrente

        q_cur      = qqq['Close'].iloc[-1]
        atr_raw    = (qqq['High'] - qqq['Low']).rolling(14).mean().iloc[-1]
        atr_perc   = (atr_raw / q_cur * 100) if not pd.isna(atr_raw) else 0.15
        soglia     = max(0.15, atr_perc * 1.5)

        v1m = ((q_cur - qqq['Close'].iloc[-2]) / qqq['Close'].iloc[-2]) * 100
        v3m = ((q_cur - qqq['Close'].iloc[-4]) / qqq['Close'].iloc[-4]) * 100

        is_breakout  = v3m >= 0.15 and v1m >= 0
        nuovo_blocco = blocco_corrente

        if v1m <= -soglia or v3m <= -soglia:
            nuovo_blocco = ora_ny + datetime.timedelta(minutes=veto_minuti)
            if invia_telegram_fn:
                invia_telegram_fn(
                    f"🚨 <b>VETO {bot_name}</b>\n"
                    f"Crollo QQQ (soglia -{soglia:.2f}%):\n"
                    f"1m: {v1m:.2f}% | 3m: {v3m:.2f}%"
                )

        return is_breakout, nuovo_blocco

    except Exception as e:
        print(f"⚠️ QQQ fetch fallito [{bot_name}]: {e}")
        return False, blocco_corrente

def is_veto_attivo(blocco_fino_a, ora_ny, qqq_dati_disponibili=True):
    """True se il veto è attivo (blocco temporale o primo ciclo QQQ non ancora completato)."""
    veto_temp = blocco_fino_a is not None and ora_ny < blocco_fino_a
    return veto_temp or not qqq_dati_disponibili

def calcola_regime_qqq(adx_trend=22, adx_bear=20, adx_range=18, periodo_adx=14):
    """Restituisce il regime di mercato corrente basato su ADX di QQQ su 15m.
    - 'trend'  : ADX >= adx_trend e +DI > -DI (trend rialzista forte)
    - 'bear'   : ADX >= adx_bear  e -DI > +DI (trend ribassista — cattura i crolli direzionali)
    - 'range'  : ADX <= adx_range (mercato laterale)
    - 'neutro' : tutto il resto
    Restituisce 'neutro' anche su errore (fail-safe: nessun cambio comportamento)."""
    try:
        import indicatori
        qqq = yf.Ticker("QQQ").history(period="5d", interval="15m", timeout=10)
        if qqq.empty or len(qqq) < periodo_adx * 2:
            return 'neutro'
        adx_s, plus_di_s, minus_di_s = indicatori.calcola_adx_completo(qqq, periodo=periodo_adx)
        adx      = float(adx_s.iloc[-1])
        plus_di  = float(plus_di_s.iloc[-1])
        minus_di = float(minus_di_s.iloc[-1])
        if adx >= adx_trend and plus_di > minus_di:
            return 'trend'
        if adx >= adx_bear and minus_di > plus_di:
            return 'bear'
        if adx <= adx_range:
            return 'range'
        return 'neutro'
    except Exception as e:
        print(f"⚠️ calcola_regime_qqq: {e}")
        return 'neutro'
