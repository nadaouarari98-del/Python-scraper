import yfinance as yf
import json
import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import yaml
from rapidfuzz import process, fuzz
import pandas as pd

CACHE_FILE = os.path.join('data', 'cache', 'market_prices.json')
SETTINGS_FILE = os.path.join('config', 'settings.yaml')

# --- Cache Management ---
def _load_cache() -> dict:
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}

def _save_cache(cache: dict):
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)

def _is_cache_valid(cached_entry: dict) -> bool:
    if not cached_entry.get('price_date'):
        return False
    try:
        cached_date = datetime.fromisoformat(cached_entry['price_date'])
    except Exception:
        return False
    days_old = (datetime.now() - cached_date).days
    return days_old <= 3  # covers weekends

# --- Settings Loader ---
def _load_company_tickers() -> dict:
    with open(SETTINGS_FILE, encoding='utf-8') as f:
        settings = yaml.safe_load(f)
    return settings.get('company_tickers', {})

# --- Main Price Fetcher ---
def get_market_price(company_name: str) -> dict:
    """
    Returns: {
        "company": str,
        "nse_price": float | None,
        "bse_price": float | None,
        "final_price": float,
        "price_date": str,
        "source": "NSE" | "BSE" | "NSE+BSE" | "not_found",
        "cached": bool
    }
    """
    cache = _load_cache()
    tickers = _load_company_tickers()
    result = {
        "company": company_name,
        "nse_price": None,
        "bse_price": None,
        "final_price": 0,
        "price_date": None,
        "source": "not_found",
        "cached": False
    }
    # Check cache
    cached = cache.get(company_name)
    if cached and _is_cache_valid(cached):
        cached["cached"] = True
        return cached
    # Fuzzy match if not found
    ticker_info = tickers.get(company_name)
    if not ticker_info:
        choices = list(tickers.keys())
        match, score, _ = process.extractOne(company_name, choices, scorer=fuzz.token_sort_ratio)
        if score > 80:
            ticker_info = tickers[match]
            result["company"] = match
        else:
            result["final_price"] = 0
            result["source"] = "not_found"
            cache[company_name] = result
            _save_cache(cache)
            return result
    nse_ticker = ticker_info.get('nse')
    bse_ticker = ticker_info.get('bse')
    nse_price = None
    bse_price = None
    price_date = None
    # Fetch prices
    try:
        if nse_ticker:
            nse_data = yf.Ticker(nse_ticker).history(period='5d')
            if not nse_data.empty and not nse_data['Close'].dropna().empty:
                nse_price = float(nse_data['Close'].dropna().iloc[-1])
                price_date = str(nse_data['Close'].dropna().index[-1].date())
        if bse_ticker:
            bse_data = yf.Ticker(bse_ticker).history(period='5d')
            if not bse_data.empty and not bse_data['Close'].dropna().empty:
                bse_price = float(bse_data['Close'].dropna().iloc[-1])
                if not price_date:
                    price_date = str(bse_data['Close'].dropna().index[-1].date())
    except Exception as e:
        pass
    # Decide final price
    if nse_price is not None and bse_price is not None:
        final_price = max(nse_price, bse_price)
        source = "NSE+BSE" if nse_price == bse_price else ("NSE" if nse_price > bse_price else "BSE")
    elif nse_price is not None:
        final_price = nse_price
        source = "NSE"
    elif bse_price is not None:
        final_price = bse_price
        source = "BSE"
    else:
        final_price = 0
        source = "not_found"
    result.update({
        "nse_price": nse_price,
        "bse_price": bse_price,
        "final_price": final_price,
        "price_date": price_date or datetime.now().date().isoformat(),
        "source": source,
        "cached": False
    })
    cache[company_name] = result
    _save_cache(cache)
    return result

# --- Batch Function ---
def get_market_prices_for_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each unique company_name in df, fetch market price.
    Adds columns to df: market_price, price_date, price_source
    Also adds: market_value = current_holding * market_price
    Also adds: total_dividend = sum of all fy_dividend_* columns
    Also adds: total_wealth = total_dividend + market_value
    """
    companies = df['company_name'].unique()
    price_map = {c: get_market_price(c) for c in companies}
    df['market_price'] = df['company_name'].map(lambda c: price_map[c]['final_price'])
    df['price_date'] = df['company_name'].map(lambda c: price_map[c]['price_date'])
    df['price_source'] = df['company_name'].map(lambda c: price_map[c]['source'])
    if 'current_holding' in df.columns:
        df['market_value'] = df['current_holding'] * df['market_price']
    else:
        df['market_value'] = 0
    # Sum all fy_dividend_* columns
    dividend_cols = [col for col in df.columns if col.startswith('fy_dividend_')]
    if dividend_cols:
        df['total_dividend'] = df[dividend_cols].sum(axis=1)
    else:
        df['total_dividend'] = 0
    df['total_wealth'] = df['total_dividend'] + df['market_value']
    return df

# --- CLI ---
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch market prices for companies.")
    parser.add_argument('--company', type=str, help='Company name to fetch price for')
    parser.add_argument('--input', type=str, help='Input Excel file to update with market prices')
    args = parser.parse_args()
    if args.company:
        res = get_market_price(args.company)
        print(json.dumps(res, indent=2, ensure_ascii=False))
    elif args.input:
        df = pd.read_excel(args.input)
        df2 = get_market_prices_for_df(df)
        out_path = args.input.replace('.xlsx', '_with_prices.xlsx')
        df2.to_excel(out_path, index=False)
        print(f"Updated file saved to {out_path}")
        print(f"Total wealth: ₹{df2['total_wealth'].sum():,.2f}")
    else:
        tickers = _load_company_tickers()
        for cname in tickers:
            res = get_market_price(cname)
            print(json.dumps(res, indent=2, ensure_ascii=False))
