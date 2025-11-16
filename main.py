import pandas as pd
import requests
import io
import yfinance as yf
import time

CSV_URLS = {
    "marketcap": "https://companiesmarketcap.com/inr/?download=csv",
    "revenue": "https://companiesmarketcap.com/inr/largest-companies-by-revenue/?download=csv",
    "earnings": "https://companiesmarketcap.com/inr/most-profitable-companies/?download=csv",
}

TOP_N = 100

def download_csv(url):
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def clean_df(df):
    df = df.copy()
    if "Name" not in df.columns:
        if "name" in df.columns:
            df["Name"] = df["name"]
        else:
            raise ValueError("Missing Name column")

    if "Symbol" not in df.columns:
        if "symbol" in df.columns:
            df["Symbol"] = df["symbol"]
        else:
            df["Symbol"] = None

    df["Name"] = df["Name"].astype(str).str.strip()
    df["Symbol"] = df["Symbol"].astype(str).str.strip()
    return df

def fetch_pe(symbol):
    try:
        t = yf.Ticker(symbol)
        pe = t.info.get("trailingPE")
        if pe is not None:
            return float(pe)
    except:
        pass

    if symbol and symbol.isalpha():
        try:
            t = yf.Ticker(symbol + ".NS")
            pe = t.info.get("trailingPE")
            if pe is not None:
                return float(pe)
        except:
            pass

    return None

print("Downloading CSVs...")

dfs = {}
for key, url in CSV_URLS.items():
    df = download_csv(url)
    df = df.head(TOP_N)
    df = clean_df(df)
    dfs[key] = df
    print(f"{key} columns:", df.columns.tolist())

names_marketcap = set(dfs["marketcap"]["Name"])
names_revenue = set(dfs["revenue"]["Name"])
names_earnings = set(dfs["earnings"]["Name"])

common_names = names_marketcap & names_revenue & names_earnings

print("Common companies count:", len(common_names))

common_df = pd.DataFrame({"Name": list(common_names)})

symbol_map = dfs["marketcap"].set_index("Name")["Symbol"].to_dict()
common_df["Symbol"] = common_df["Name"].map(lambda x: symbol_map.get(x, None))

pe_values = []
print("\nFetching P/E values...")

for i, row in common_df.iterrows():
    name = row["Name"]
    symbol = row["Symbol"]
    pe = None

    if symbol not in ["None", None, "", "nan"]:
        pe = fetch_pe(symbol)

    pe_values.append(pe)
    print(f"{i+1}/{len(common_df)} | {name} ({symbol}) → PE: {pe}")
    time.sleep(0.25)

common_df["PE"] = pe_values
common_df = common_df.sort_values(by="PE", na_position="last")

common_df.to_csv("common_with_sorted_pe.csv", index=False)

print("\nSaved: common_with_sorted_pe.csv")
