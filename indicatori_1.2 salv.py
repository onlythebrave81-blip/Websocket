import pandas as pd

# --- RSI (metodo Wilder - EWM alpha=1/periodo) ---
def calcola_rsi(df, periodo=14, colonna='Close'):
    delta = df[colonna].diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/periodo, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1/periodo, adjust=False).mean()
    return 100 - (100 / (1 + gain / loss))

# --- ATR (True Range rolling) ---
def calcola_atr(df, periodo=14):
    tr = pd.concat([
        df['High'] - df['Low'],
        (df['High'] - df['Close'].shift()).abs(),
        (df['Low']  - df['Close'].shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(periodo).mean()

# --- VWAP semplice (intraday cumulativo su singola sessione) ---
def calcola_vwap(df):
    tipico = (df['High'] + df['Low'] + df['Close']) / 3
    return (df['Volume'] * tipico).cumsum() / df['Volume'].cumsum()

# --- VWAP per data (multi-day, più preciso — usato da Intraday su 15m) ---
def calcola_vwap_per_data(df):
    tipico   = (df['High'] + df['Low'] + df['Close']) / 3
    vol_tip  = tipico * df['Volume']
    return vol_tip.groupby(df.index.date).cumsum() / df['Volume'].groupby(df.index.date).cumsum()

# --- MACD ---
def calcola_macd(df, fast=12, slow=26, signal=9, colonna='Close'):
    exp1  = df[colonna].ewm(span=fast,   adjust=False).mean()
    exp2  = df[colonna].ewm(span=slow,   adjust=False).mean()
    macd  = exp1 - exp2
    sig   = macd.ewm(span=signal, adjust=False).mean()
    return macd, sig

# --- EMA generica ---
def calcola_ema(df, span, colonna='Close'):
    return df[colonna].ewm(span=span, adjust=False).mean()

# --- Bollinger Bands (lower, upper) ---
def calcola_bollinger(df, periodo=20, colonna='Close'):
    sma = df[colonna].rolling(periodo).mean()
    std = df[colonna].rolling(periodo).std()
    return sma - (2 * std), sma + (2 * std)

# --- Donchian High ---
def calcola_donchian_high(df, periodo=20):
    return df['High'].rolling(periodo).max()

# --- Volume medio (SMA) ---
def calcola_vol_medio(df, periodo=20):
    return df['Volume'].rolling(periodo).mean()

# --- ADX (Average Directional Index — metodo Wilder) ---
def calcola_adx(df, periodo=14):
    high  = df['High']
    low   = df['Low']
    close = df['Close']

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    up   = high.diff()
    down = -low.diff()

    plus_dm  = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)

    atr_s    = tr.ewm(alpha=1/periodo, adjust=False).mean()
    plus_di  = 100 * plus_dm.ewm(alpha=1/periodo, adjust=False).mean() / atr_s
    minus_di = 100 * minus_dm.ewm(alpha=1/periodo, adjust=False).mean() / atr_s

    dx  = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1)
    adx = dx.ewm(alpha=1/periodo, adjust=False).mean()

    return adx

# --- ADX completo (restituisce anche +DI e -DI per regime detector) ---
def calcola_adx_completo(df, periodo=14):
    """Restituisce (adx, plus_di, minus_di) come Serie pandas.
    Usato dal regime detector che ha bisogno della direzione del trend."""
    high  = df['High']
    low   = df['Low']
    close = df['Close']

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    up   = high.diff()
    down = -low.diff()

    plus_dm  = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)

    atr_s    = tr.ewm(alpha=1/periodo, adjust=False).mean()
    plus_di  = 100 * plus_dm.ewm(alpha=1/periodo, adjust=False).mean() / atr_s
    minus_di = 100 * minus_dm.ewm(alpha=1/periodo, adjust=False).mean() / atr_s

    dx  = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1)
    adx = dx.ewm(alpha=1/periodo, adjust=False).mean()

    return adx, plus_di, minus_di

# --- Forza relativa vs benchmark (es. QQQ) ---
def calcola_forza_relativa(df_ticker, df_benchmark, soglia=0.002, finestra=4):
    """True se il ticker ha sovraperformato il benchmark di almeno `soglia`
    nelle ultime `finestra` candele. Default: 0.2% su 4 candele 15m = 60min.
    Restituisce False se i dati sono insufficienti (fail-closed: filtro hard)."""
    try:
        if df_ticker is None or df_benchmark is None:
            return False
        if len(df_ticker) < finestra + 1 or len(df_benchmark) < finestra + 1:
            return False
        perf_t = (df_ticker['Close'].iloc[-1] / df_ticker['Close'].iloc[-(finestra+1)]) - 1
        perf_b = (df_benchmark['Close'].iloc[-1] / df_benchmark['Close'].iloc[-(finestra+1)]) - 1
        return (perf_t - perf_b) >= soglia
    except Exception:
        return False
