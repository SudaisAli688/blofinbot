import ccxt.pro as ccxtpro
import ccxt
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import datetime
from datetime import timezone, timedelta
import os
import asyncio
import httpx

# =========================
# ⚙️ Config
# =========================
# IMPORTANT: Replace with your actual token and chat ID, or set them as environment variables.
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8277544471:AAGhVkgju8P3a06fNh87lN679JnPa6RkcwI"  )
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "7778420928")
LOG_FILE = "multi_timeframe_signals_log.csv"

# Concurrency setting: How many symbols to scan at the same time.
# A lower number is safer for avoiding rate limits with a large watchlist.
MAX_CONCURRENT_SCANS = 2

WATCHLIST = [
    "1000000BABYDOGE/USDT:USDT", "1000000MOG/USDT:USDT", "10000SATS/USDT:USDT", "1000BONK/USDT:USDT",
    "1000CHEEMS/USDT:USDT", "1000FLOKI/USDT:USDT", "1000RATS/USDT:USDT", "1INCH/USDT:USDT", "0G/USDT:USDT",
    "2Z/USDT:USDT", "A2Z/USDT:USDT", "AAVE/USDT:USDT", "AA/USDT:USDT", "ACH/USDT:USDT", "ACT/USDT:USDT", "ADA/USDT:USDT",
    "AERGO/USDT:USDT", "AERO/USDT:USDT", "AEVO/USDT:USDT", "AGI/USDT:USDT", "AGT/USDT:USDT", "AI/USDT:USDT",
    "AI16Z/USDT:USDT", "AIN/USDT:USDT", "AIO/USDT:USDT", "AIOT/USDT:USDT", "AIXBT/USDT:USDT", "AIOZ/USDT:USDT",
    "AKE/USDT:USDT", "ALCH/USDT:USDT", "ALGO/USDT:USDT", "ANKR/USDT:USDT", "APEX/USDT:USDT", "API3/USDT:USDT",
    "ARC/USDT:USDT", "ARB/USDT:USDT", "ARIA/USDT:USDT", "ARKM/USDT:USDT", "AR/USDT:USDT", "ASR/USDT:USDT",
    "ASTER/USDT:USDT", "ASTR/USDT:USDT", "ATH/USDT:USDT", "ATOM/USDT:USDT", "A/USDT:USDT", "AVA/USDT:USDT",
    "AVAAI/USDT:USDT", "AVAX/USDT:USDT", "AVNT/USDT:USDT", "AWE/USDT:USDT", "AXS/USDT:USDT", "BABY/USDT:USDT",
    "BAN/USDT:USDT", "BANANA/USDT:USDT", "BANANAS31/USDT:USDT", "BANK/USDT:USDT", "BARD/USDT:USDT",
    "BAS/USDT:USDT", "BCH/USDT:USDT", "BEAM/USDT:USDT", "BEL/USDT:USDT", "BERA/USDT:USDT", "BID/USDT:USDT",
    "BIGTIME/USDT:USDT", "BIO/USDT:USDT", "BLAST/USDT:USDT", "BLESS/USDT:USDT", "BLUR/USDT:USDT", "BNB/USDT:USDT",
    "BOME/USDT:USDT", "BRETT/USDT:USDT", "BTCDOM/USDT:USDT", "BTC/USDT:USDT", "BTR/USDT:USDT", "B/USDT:USDT",
    "C98/USDT:USDT", "CARV/USDT:USDT", "CATI/USDT:USDT", "CETUS/USDT:USDT", "CGPT/USDT:USDT",
    "CHILLGUY/USDT:USDT", "CKB/USDT:USDT", "COAI/USDT:USDT", "COOKIE/USDT:USDT", "COW/USDT:USDT", "CRO/USDT:USDT",
    "CRV/USDT:USDT", "C/USDT:USDT", "CUDIS/USDT:USDT", "CVX/USDT:USDT", "DAM/USDT:USDT", "DEEP/USDT:USDT",
    "DEGEN/USDT:USDT", "DIA/USDT:USDT", "DOG/USDT:USDT", "DOGE/USDT:USDT", "DOOD/USDT:USDT", "DOT/USDT:USDT",
    "DRIFT/USDT:USDT", "EDEN/USDT:USDT", "EIGEN/USDT:USDT", "ENA/USDT:USDT", "ENS/USDT:USDT",
    "ESPORTS/USDT:USDT", "ETC/USDT:USDT", "ETHBTC/USDT:USDT", "ETHFI/USDT:USDT", "ETH/USDT:USDT", "EPT/USDT:USDT",
    "ERA/USDT:USDT", "EVAA/USDT:USDT", "FARTCOIN/USDT:USDT", "FET/USDT:USDT", "FF/USDT:USDT", "FIS/USDT:USDT",
    "FLM/USDT:USDT", "FLUID/USDT:USDT", "FORM/USDT:USDT", "FORTH/USDT:USDT", "FWOG/USDT:USDT", "FUN/USDT:USDT",
    "GMX/USDT:USDT", "GOAT/USDT:USDT", "GRASS/USDT:USDT", "GRIFFIN/USDT:USDT", "GUN/USDT:USDT", "G/USDT:USDT",
    "HAEDAL/USDT:USDT", "HANA/USDT:USDT", "HBAR/USDT:USDT", "HEMI/USDT:USDT", "HIGH/USDT:USDT", "HIPPO/USDT:USDT",
    "HMSTR/USDT:USDT", "HOLO/USDT:USDT", "HOME/USDT:USDT", "HOOK/USDT:USDT", "HOT/USDT:USDT", "H/USDT:USDT",
    "HUMA/USDT:USDT", "HYPE/USDT:USDT", "HYPER/USDT:USDT", "ICNT/USDT:USDT", "ICP/USDT:USDT", "ID/USDT:USDT",
    "IDOL/USDT:USDT", "ILV/USDT:USDT", "INJ/USDT:USDT", "INIT/USDT:USDT", "IN/USDT:USDT", "IO/USDT:USDT",
    "IOTA/USDT:USDT", "IOTX/USDT:USDT", "IP/USDT:USDT", "JASMY/USDT:USDT", "JELLYJELLY/USDT:USDT",
    "JOE/USDT:USDT", "JTO/USDT:USDT", "JUP/USDT:USDT", "KAIA/USDT:USDT", "KAITO/USDT:USDT", "KAS/USDT:USDT",
    "KAVA/USDT:USDT", "KERNEL/USDT:USDT", "KMNO/USDT:USDT", "K/USDT:USDT", "LA/USDT:USDT", "LAYER/USDT:USDT",
    "LDO/USDT:USDT", "LIGHT/USDT:USDT", "LINEA/USDT:USDT", "LINK/USDT:USDT", "LISTA/USDT:USDT", "LPT/USDT:USDT",
    "LQTY/USDT:USDT", "LTC/USDT:USDT", "LUMIA/USDT:USDT", "MANTA/USDT:USDT", "MELANIA/USDT:USDT",
    "MERL/USDT:USDT", "MILK/USDT:USDT", "MIRA/USDT:USDT", "MITO/USDT:USDT", "MLN/USDT:USDT", "MNT/USDT:USDT",
    "MOODENG/USDT:USDT", "MORPH/USDT:USDT", "MOVE/USDT:USDT", "MUBARAK/USDT:USDT", "M/USDT:USDT",
    "MYRO/USDT:USDT", "MYX/USDT:USDT", "NAORIS/USDT:USDT", "NEAR/USDT:USDT", "NEIRO/USDT:USDT", "NEWT/USDT:USDT",
    "NIL/USDT:USDT", "NMR/USDT:USDT", "NOM/USDT:USDT", "NOT/USDT:USDT", "OBOL/USDT:USDT", "OL/USDT:USDT", "OM/USDT:USDT",
    "ONDO/USDT:USDT", "ONE/USDT:USDT", "OPEN/USDT:USDT", "ORDER/USDT:USDT", "ORCA/USDT:USDT", "PARTI/USDT:USDT",
    "PENDLE/USDT:USDT", "PENGU/USDT:USDT", "PEPE/USDT:USDT", "PHA/USDT:USDT", "PHB/USDT:USDT", "PIXEL/USDT:USDT",
    "PIPPIN/USDT:USDT", "PLUME/USDT:USDT", "PNUT/USDT:USDT", "POL/USDT:USDT", "POLYX/USDT:USDT",
    "PONKE/USDT:USDT", "POPCAT/USDT:USDT", "PORTAL/USDT:USDT", "PRIME/USDT:USDT", "PROM/USDT:USDT",
    "PROMPT/USDT:USDT", "PROVE/USDT:USDT", "PTB/USDT:USDT", "PUMP/USDT:USDT", "PUNDIX/USDT:USDT",
    "PYTH/USDT:USDT", "QNT/USDT:USDT", "Q/USDT:USDT", "RPL/USDT:USDT", "RARE/USDT:USDT", "RAY/USDT:USDT",
    "RED/USDT:USDT", "REI/USDT:USDT", "RENDER/USDT:USDT", "RESOLV/USDT:USDT", "RLC/USDT:USDT", "RUNE/USDT:USDT",
    "SAHARA/USDT:USDT", "SAPIEN/USDT:USDT", "SCR/USDT:USDT", "SEI/USDT:USDT", "SHIB/USDT:USDT", "SIGN/USDT:USDT",
    "SKATE/USDT:USDT", "SKY/USDT:USDT", "SOMI/USDT:USDT", "SOL/USDT:USDT", "SOON/USDT:USDT", "SOPH/USDT:USDT",
    "SPK/USDT:USDT", "SPX/USDT:USDT", "SQD/USDT:USDT", "SSV/USDT:USDT", "STBL/USDT:USDT", "STX/USDT:USDT",
    "STRK/USDT:USDT", "SUI/USDT:USDT", "SUN/USDT:USDT", "SUPER/USDT:USDT", "S/USDT:USDT", "SWARMS/USDT:USDT",
    "SWELL/USDT:USDT", "SXT/USDT:USDT", "SYRUP/USDT:USDT", "TAC/USDT:USDT", "TAG/USDT:USDT", "TAI/USDT:USDT",
    "TAKE/USDT:USDT", "TAO/USDT:USDT", "TAU/USDT:USDT", "TNSR/USDT:USDT", "THE/USDT:USDT", "THETA/USDT:USDT",
    "TIA/USDT:USDT", "TOKEN/USDT:USDT", "TON/USDT:USDT", "TOSHI/USDT:USDT", "TOWNS/USDT:USDT",
    "TRADOOR/USDT:USDT", "TREE/USDT:USDT", "TRB/USDT:USDT", "TRUMP/USDT:USDT", "TRUTH/USDT:USDT", "TRX/USDT:USDT",
    "TST/USDT:USDT", "T/USDT:USDT", "TUTU/USDT:USDT", "TWT/USDT:USDT", "TURBO/USDT:USDT", "U/USDT:USDT", "UMA/USDT:USDT",
    "USELESS/USDT:USDT", "USUAL/USDT:USDT", "VANRY/USDT:USDT", "VELODROME/USDT:USDT", "VELVET/USDT:USDT",
    "VET/USDT:USDT", "VFY/USDT:USDT", "VINE/USDT:USDT", "VIRTUAL/USDT:USDT", "VVV/USDT:USDT", "WAL/USDT:USDT",
    "WAVES/USDT:USDT", "WCT/USDT:USDT", "WIF/USDT:USDT", "WLD/USDT:USDT", "WLFI/USDT:USDT", "WOO/USDT:USDT",
    "W/USDT:USDT", "XAI/USDT:USDT", "XAN/USDT:USDT", "XAUT/USDT:USDT", "XLM/USDT:USDT", "XPL/USDT:USDT",
    "XPIN/USDT:USDT", "XRP/USDT:USDT", "XTZ/USDT:USDT", "XVG/USDT:USDT", "XNY/USDT:USDT", "YALA/USDT:USDT",
    "YFI/USDT:USDT", "YGG/USDT:USDT", "YZY/USDT:USDT", "ZBCN/USDT:USDT", "ZEC/USDT:USDT", "ZEN/USDT:USDT",
    "ZEREBRO/USDT:USDT", "ZK/USDT:USDT", "ZKC/USDT:USDT", "ZKJ/USDT:USDT", "ZORA/USDT:USDT", "ZRC/USDT:USDT",
    "ZRO/USDT:USDT", "ZRX/USDT:USDT"
]

# =========================
# 🔧 Exchange Setup
# =========================
def get_exchange():
    """Initializes a Blofin instance with longer timeout and rate limit."""
    return ccxtpro.blofin({
        'options': {'defaultType': 'swap'},
        'enableRateLimit': True,
        'timeout': 30000,  # 30 seconds
    })

async def load_markets_with_retry(exchange, market_type):
    """Load markets with retry to prevent timeout errors."""
    for attempt in range(3):
        try:
            await exchange.load_markets(params={'type': market_type})
            return
        except (ccxt.base.errors.RequestTimeout, ccxt.base.errors.RateLimitExceeded) as e:
            print(f"⏳ Timeout/Rate limit loading {market_type} markets, retrying ({attempt+1}/3)... Error: {e}")
            await asyncio.sleep(10 * (attempt + 1)) # Exponential backoff
    print(f"❌ Failed to load {market_type} markets after 3 retries.")

# =========================
# 📩 Telegram & 📝 Logging
# =========================
async def send_telegram_message(text, img_path=None):
    if not TELEGRAM_TOKEN or "YOUR_TELEGRAM_TOKEN" in TELEGRAM_TOKEN:
        print("❌ Telegram token not set. Skipping message.")
        return
    try:
        async with httpx.AsyncClient(timeout=40.0  ) as client:
            if img_path and os.path.exists(img_path):
                url_photo = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
                with open(img_path, "rb"  ) as f:
                    files = {"photo": f}
                    data = {"chat_id": TELEGRAM_CHAT_ID, "caption": text, "parse_mode": "Markdown"}
                    res = await client.post(url_photo, files=files, data=data)
                    res.raise_for_status()
                    print(f"✅ Successfully sent alert for {text.splitlines()[0]}")
            else:
                url_text = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                data = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
                res = await client.post(url_text, data=data  )
                res.raise_for_status()
    except Exception as e:
        print(f"❌ Telegram send failed: {e}")

def log_signal(symbol, date, signal_type, profile):
    now = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    clean_symbol = symbol.split(':')[0]
    new_row = {"symbol": clean_symbol, "date": date, "signal_type": signal_type, "profile": profile, "scan_time": now}
    df = pd.DataFrame([new_row])
    try:
        if not os.path.exists(LOG_FILE):
            df.to_csv(LOG_FILE, index=False)
        else:
            df.to_csv(LOG_FILE, mode="a", index=False, header=False)
    except Exception as e:
        print(f"❌ Failed to write to log file: {e}")

# =========================
# 📊 Chart Plotting
# =========================
def plot_candles(df, symbol, signal_type, ref_level=None, direction="bullish"):
    df_plot = df.copy().tail(14)
    df_plot.set_index("timestamp", inplace=True)

    mc = mpf.make_marketcolors(up='White', down='Black', wick='black', edge='black')
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle='-', gridcolor='lightgray')

    clean_symbol = symbol.split(':')[0]
    path = f"{clean_symbol.replace('/', '')}_chart.png"

    fig, axlist = mpf.plot(
        df_plot, type='candle', style=s, title=f"{clean_symbol} - {signal_type} (1D Chart)",
        ylabel='Price', volume=False, returnfig=True, figsize=(10,10), tight_layout=True
    )

    ax = axlist[0]
    for spine in ax.spines.values():
        spine.set_visible(False)

    if ref_level is not None:
        xmin, xmax = ax.get_xlim()
        label = " CRT Low" if direction == "bullish" else " CRT High"
        ax.hlines(ref_level, xmin=xmin, xmax=xmax, linestyle='-', color='black', linewidth=1.5)
        ax.text(
            xmin, ref_level, label,
            va=('bottom' if direction == 'bullish' else 'top'),
            ha='left', fontsize=9, color='black', weight='bold'
        )

    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path

# =========================
# 📢 Alert Formatter
# =========================
def make_alert(signal_type, symbol, profile):
    clean_symbol = symbol.split(':')[0]
    profile_name = "Anchor Close" if profile == "A" else "Wick Sweep"
    
    if signal_type == "Bullish":
        title = f"🔥 Bullish Signal: {clean_symbol}"
    else:
        title = f"⚡️ Bearish Signal: {clean_symbol}"
        
    tv_symbol_param = f"BLOFIN:{clean_symbol.replace('/', '')}.P"
    tv_url = f"https://www.tradingview.com/chart/?symbol={tv_symbol_param}"
    
    caption = (
        f"{title}\n"
        f"**Strategy Profile:** {profile_name} Reversal\n"
        f"🔗 [Chart on TradingView]({tv_url}  )"
    )
    return caption

# =========================
# 📈 OHLCV Helper
# =========================
async def get_ohlcv(exchange, symbol, timeframe="1d", limit=50, since=None):
    params = {'limit': limit}
    if since:
        params['since'] = since
    try:
        market_type = 'swap' if ':' in symbol else 'spot'
        # The built-in rate limiter in CCXT will handle waiting if we get a RateLimitExceeded error
        data = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, params={'type': market_type}, **params)
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except ccxt.RateLimitExceeded as e:
        print(f"  - Rate limit exceeded for {symbol}. CCXT's built-in handler will wait. Error: {e}")
        # The library's rate limiter should handle the delay, but we can add a small extra sleep just in case.
        await asyncio.sleep(5)
        return pd.DataFrame() # Return empty to signify failure for this attempt
    except Exception as e:
        print(f"  - Could not fetch OHLCV for {symbol} on {timeframe}: {e}")
        return pd.DataFrame()

# =========================
# 🔍 DUAL PROFILE Signal Logic (with Monthly Confirmation)
# =========================
async def check_signals_dual_profile(exchange, symbol, semaphore):
    async with semaphore:
        try:
            print(f"🔍 Scanning {symbol}...")
            
            monthly_df = await get_ohlcv(exchange, symbol, "1M", 3)
            if len(monthly_df) < 2:
                print(f"  - Not enough monthly data for {symbol}.")
                return
            
            previous_month = monthly_df.iloc[-2]
            previous_month_high = previous_month["high"]
            previous_month_low = previous_month["low"]

            weekly_df = await get_ohlcv(exchange, symbol, "1w", 4)
            if len(weekly_df) < 3:
                print(f"  - Not enough weekly data for {symbol}.")
                return
                
            signal_week = weekly_df.iloc[-2]
            reference_week = weekly_df.iloc[-3]
            
            daily_df_signal_week = await get_ohlcv(exchange, symbol, "1d", limit=7, since=int(signal_week["timestamp"].timestamp() * 1000))
            if len(daily_df_signal_week) < 2:
                print(f"  - Not enough daily data in the signal week for {symbol}.")
                return

            # --- Bullish Signal Profiles ---
            if signal_week["close"] > reference_week["low"] and signal_week["low"] < previous_month_low:
                # Profile A: Anchor Close
                anchor_candle_A = None
                anchor_index_A = -1
                for i, candle in daily_df_signal_week.iterrows():
                    if candle["close"] < reference_week["low"]:
                        anchor_candle_A = candle
                        anchor_index_A = i
                        break
                if anchor_candle_A is not None:
                    for i in range(anchor_index_A + 1, len(daily_df_signal_week)):
                        if daily_df_signal_week.iloc[i]["close"] > anchor_candle_A["open"]:
                            print(f"🔥 Bullish Profile A (Anchor Close) Confirmed for {symbol}!")
                            daily_df_plot = await get_ohlcv(exchange, symbol, "1d", 14, int(reference_week["timestamp"].timestamp() * 1000))
                            chart_path = plot_candles(daily_df_plot, symbol, "Bullish Expansion Setup", reference_week["low"], "bullish")
                            caption = make_alert("Bullish", symbol, "A")
                            await send_telegram_message(caption, chart_path)
                            log_signal(symbol, str(signal_week['timestamp'].date()), "Bullish", "A")
                            return

                # Profile B: Wick Sweep
                sweep_candle_B = None
                sweep_index_B = -1
                for i, candle in daily_df_signal_week.iterrows():
                    if candle["low"] < reference_week["low"] and candle["close"] >= reference_week["low"]:
                        sweep_candle_B = candle
                        sweep_index_B = i
                        break
                if sweep_candle_B is not None:
                    for i in range(sweep_index_B + 1, len(daily_df_signal_week)):
                        if daily_df_signal_week.iloc[i]["close"] > sweep_candle_B["high"]:
                            print(f"🔥 Bullish Profile B (Wick Sweep) Confirmed for {symbol}!")
                            daily_df_plot = await get_ohlcv(exchange, symbol, "1d", 14, int(reference_week["timestamp"].timestamp() * 1000))
                            chart_path = plot_candles(daily_df_plot, symbol, "Bullish Expansion Setup", reference_week["low"], "bullish")
                            caption = make_alert("Bullish", symbol, "B")
                            await send_telegram_message(caption, chart_path)
                            log_signal(symbol, str(signal_week['timestamp'].date()), "Bullish", "B")
                            return

            # --- Bearish Signal Profiles ---
            if signal_week["close"] < reference_week["high"] and signal_week["high"] > previous_month_high:
                # Profile A: Anchor Close
                anchor_candle_A = None
                anchor_index_A = -1
                for i, candle in daily_df_signal_week.iterrows():
                    if candle["close"] > reference_week["high"]:
                        anchor_candle_A = candle
                        anchor_index_A = i
                        break
                if anchor_candle_A is not None:
                    for i in range(anchor_index_A + 1, len(daily_df_signal_week)):
                        if daily_df_signal_week.iloc[i]["close"] < anchor_candle_A["open"]:
                            print(f"⚡️ Bearish Profile A (Anchor Close) Confirmed for {symbol}!")
                            daily_df_plot = await get_ohlcv(exchange, symbol, "1d", 14, int(reference_week["timestamp"].timestamp() * 1000))
                            chart_path = plot_candles(daily_df_plot, symbol, "Bearish Expansion Setup", reference_week["high"], "bearish")
                            caption = make_alert("Bearish", symbol, "A")
                            await send_telegram_message(caption, chart_path)
                            log_signal(symbol, str(signal_week['timestamp'].date()), "Bearish", "A")
                            return

                # Profile B: Wick Sweep
                sweep_candle_B = None
                sweep_index_B = -1
                for i, candle in daily_df_signal_week.iterrows():
                    if candle["high"] > reference_week["high"] and candle["close"] <= reference_week["high"]:
                        sweep_candle_B = candle
                        sweep_index_B = i
                        break
                if sweep_candle_B is not None:
                    for i in range(sweep_index_B + 1, len(daily_df_signal_week)):
                        if daily_df_signal_week.iloc[i]["close"] < sweep_candle_B["low"]:
                            print(f"⚡️ Bearish Profile B (Wick Sweep) Confirmed for {symbol}!")
                            daily_df_plot = await get_ohlcv(exchange, symbol, "1d", 14, int(reference_week["timestamp"].timestamp() * 1000))
                            chart_path = plot_candles(daily_df_plot, symbol, "Bearish Expansion Setup", reference_week["high"], "bearish")
                            caption = make_alert("Bearish", symbol, "B")
                            await send_telegram_message(caption, chart_path)
                            log_signal(symbol, str(signal_week['timestamp'].date()), "Bearish", "B")
                            return

            print(f"  - ✅ {symbol} Done (no confirmed signals).")
        except Exception as e:
            print(f"❌ An unexpected error occurred with {symbol}: {e}")

# =========================
# 🚀 Job Runner
# =========================
async def weekly_job():
    print("\n" + "="*50)
    start_time = datetime.datetime.now(timezone.utc)
    print(f"🚀 Starting Multi-Timeframe Weekly Scan at {start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC...")
    print(f"Scanning {len(WATCHLIST)} tokens with a concurrency of {MAX_CONCURRENT_SCANS}.")
    print("="*50)
    
    exchange = get_exchange()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SCANS)
    
    try:
        await load_markets_with_retry(exchange, 'swap')

        tasks = [check_signals_dual_profile(exchange, symbol, semaphore) for symbol in WATCHLIST]
        await asyncio.gather(*tasks)

        end_time = datetime.datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        print(f"\n✅ Weekly Scan Complete at {end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"Total scan duration: {duration:.2f} seconds ({duration/60:.2f} minutes).")
    finally:
        await exchange.close()

# =========================
# 🔧 Main Loop
# =========================
async def main_loop():
    print("Bot started.")
    print("🚀 Performing initial scan on startup...")
    await weekly_job()
    print("✅ Initial scan complete.")

    while True:
        now = datetime.datetime.now(timezone.utc)
        days_until_monday = (7 - now.weekday()) % 7
        if days_until_monday == 0 and now.time() > datetime.time(0, 5):
             days_until_monday = 7
        
        next_scan_time = (now + timedelta(days=days_until_monday)).replace(hour=0, minute=5, second=0, microsecond=0)
        wait_seconds = (next_scan_time - now).total_seconds()
        
        print(f"Bot is live. ⏳ Next weekly scan scheduled for {next_scan_time} UTC.")
        print(f"Sleeping for {wait_seconds / 3600:.2f} hours...")
        await asyncio.sleep(wait_seconds)
        await weekly_job()

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
