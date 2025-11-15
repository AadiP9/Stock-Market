# main.py
# Python 3.10+ recommended

import io
import time
import requests
import pandas as pd
import yfinance as yf  # pip install yfinance
from pathlib import Path

OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)

# ------------- helper ----------------
def download_csv(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

# ------------- URLs (site supports ?download=csv) -------------
# market cap full list (we'll filter country=='India')
MC_URL = "https://companiesmarketcap.com/inr?download=csv"
# revenue (global) CSV — will filter for India rows
REV_URL = "https://companiesmarketcap.com/largest-companies-by-revenue?download=csv"
# earnings/profitability CSV (global) — will filter for India rows
EARN_URL = "https://companiesmarketcap.com/most-profitable-companies/?download=csv"

print("Downloading CSVs...")
mc_df = download_csv(MC_URL)
rev_df = download_csv(REV_URL)
earn_df = download_csv(EARN_URL)

# Inspect columns
print("marketcap columns:", mc_df.columns.tolist()[:10])
print("revenue columns:", rev_df.columns.tolist()[:10])
print("earnings columns:", earn_df.columns.tolist()[:10])

# Normalize column names to lowercase for robustness
def norm(df):
    df.columns = [c.strip() for c in df.columns]
    return df

mc_df = norm(mc_df)
rev_df = norm(rev_df)
earn_df = norm(earn_df)

# ------------- Filter to India and top 100 -------------
def top100_by_rank_or_col(df, country_col='country', country_name='India', rank_col_candidates=('Rank','rank','rank ')):
    # prefer an integer rank column if present, else use sort by marketcap/revenue/earnings column descending
    df_country = df[df['country'].str.strip().str.lower() == country_name.lower()].copy()
    # try to keep original order if 'Rank' exists
    for c in df_country.columns:
        if c.lower().strip() == 'rank':
            df_country = df_country.sort_values(by=c, key=lambda s: pd.to_numeric(s, errors='coerce')).head(100)
            return df_country.head(100)
    # fallback: use first numeric numeric-looking column except price/symbol
    numeric_cols = [c for c in df_country.columns if df_country[c].dtype != object]
    if numeric_cols:
        # sort by first numeric col descending
        df_country = df_country.sort_values(by=numeric_cols[0], ascending=False).head(100)
    else:
        df_country = df_country.head(100)
    return df_country.head(100)

top100_mc = top100_by_rank_or_col(mc_df)
top100_rev = top100_by_rank_or_col(rev_df)
top100_earn = top100_by_rank_or_col(earn_df)

# Save intermediate files
top100_mc.to_csv(OUT_DIR/"top100_marketcap_india.csv", index=False)
top100_rev.to_csv(OUT_DIR/"top100_revenue_india.csv", index=False)
top100_earn.to_csv(OUT_DIR/"top100_earnings_india.csv", index=False)

# ------------- Find common companies -------------
# Prefer to use ticker Symbol column if available, else Name.
def identify_key(df):
    for c in ['Symbol', 'symbol', 'SYMBOL', 'Ticker', 'ticker']:
        if c in df.columns:
            return c
    if 'Name' in df.columns:
        return 'Name'
    return df.columns[0]

k1 = identify_key(top100_mc)
k2 = identify_key(top100_rev)
k3 = identify_key(top100_earn)

s1 = set(top100_mc[k1].astype(str).str.strip())
s2 = set(top100_rev[k2].astype(str).str.strip())
s3 = set(top100_earn[k3].astype(str).str.strip())

common = sorted(list(s1 & s2 & s3))
print(f"Common companies count: {len(common)}")

# Build a DataFrame for the common set using symbol/ticker where possible
# We'll prefer symbol column; if keys mismatch, we'll try to join by Name
def build_common_df():
    # unify to 'symbol' and 'name' columns when possible
    # Create small helper copies with lowercase name
    def to_small(df):
        d = df.copy()
        cols = {c:c for c in df.columns}
        # standardize symbol and name
        sym = None
        for c in df.columns:
            if c.lower()=='symbol' or c.lower()=='ticker':
                sym = c
                break
        name_col = None
        for c in df.columns:
            if c.lower()=='name':
                name_col = c
                break
        d['__sym__'] = d[sym].astype(str) if sym else ""
        d['__name__'] = d[name_col].astype(str) if name_col else ""
        return d[['__sym__','__name__'] + [c for c in d.columns if c not in ('__sym__','__name__')]]
    a = to_small(top100_mc)
    b = to_small(top100_rev)
    c = to_small(top100_earn)
    # Start from symbols found in common; prefer symbol match
    common_syms = [x for x in common if x.strip()!='']
    if common_syms:
        # gather rows from each that match
        rows = []
        for sym in common_syms:
            row = {'symbol': sym}
            # try to get company name from any df
            for df in (a,b,c):
                matched = df[df['__sym__'].str.strip()==sym]
                if not matched.empty:
                    row['name'] = matched.iloc[0]['__name__']
                    break
            rows.append(row)
        return pd.DataFrame(rows)
    # else fallback to names
    rows = []
    for name in common:
        rows.append({'symbol': '', 'name': name})
    return pd.DataFrame(rows)

common_df = build_common_df()
common_df.to_csv(OUT_DIR/"common_companies.csv", index=False)

# ------------- Fetch P/E ratios -------------
# We'll try to use ticker (with exchange suffix e.g. .NS) via yfinance.
def fetch_pe(ticker):
    try:
        t = yf.Ticker(ticker.strip())
        info = t.info
        # many variants: trailingPE, trailingPegRatio, priceToEarnings?
        pe = info.get('trailingPE') or info.get('trailingPE') or info.get('priceToEarnings') or info.get('trailingPegRatio')
        # if none, try fast fallback: compute from info['regularMarketPrice']/info['epsTrailingTwelveMonths']
        if pe is None:
            price = info.get('regularMarketPrice')
            eps = info.get('epsTrailingTwelveMonths')
            if price and eps:
                pe = price / eps if eps != 0 else None
        return pe
    except Exception as e:
        print("yfinance error for", ticker, e)
        return None

# If the file has blank symbols, attempt to derive symbol by appending ".NS" to common names (risky)
pes = []
for idx, row in common_df.iterrows():
    sym = row.get('symbol','').strip()
    name = row.get('name','').strip()
    if sym:
        pe = fetch_pe(sym)
        # small pause to be polite
        time.sleep(1)
    else:
        # try naive symbol derivation (user should refine). We'll skip to avoid false positives.
        pe = None
    pes.append(pe)

common_df['pe'] = pes
common_df = common_df.sort_values(by='pe', na_position='last').reset_index(drop=True)

common_df.to_csv(OUT_DIR/"common_with_pe_sorted.csv", index=False)
# common_df.to_excel(OUT_DIR/"common_with_pe_sorted.xlsx", index=False)

print("Done. Outputs in outputs/ directory")
