import json

# Check if we can find EBIT/PPE data for a known S&P 500 stock
tickers_to_check = ["AAPL", "MSFT", "JNJ", "WMT", "PG"]

# Load stock data
nyse_stocks = []
nasdaq_stocks = []

with open('nyse_data.jsonl', 'r') as f:
    for line in f:
        if line.strip():
            try:
                nyse_stocks.append(json.loads(line))
            except:
                pass

with open('nasdaq_data.jsonl', 'r') as f:
    for line in f:
        if line.strip():
            try:
                nasdaq_stocks.append(json.loads(line))
            except:
                pass

all_stocks = nyse_stocks + nasdaq_stocks
stock_dict = {s.get("symbol", "").upper(): s for s in all_stocks}

print(f"Loaded {len(stock_dict)} stocks")

for ticker in tickers_to_check:
    if ticker in stock_dict:
        stock = stock_dict[ticker]
        data = stock.get("data", {})
        
        print(f"\n{ticker}:")
        print(f"  Has data: {'data' in stock}")
        
        # Check period dates
        period_dates = None
        for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
            if date_key in data:
                period_dates = data[date_key]
                print(f"  Found {date_key}: {len(period_dates) if isinstance(period_dates, list) else 'N/A'} entries")
                if isinstance(period_dates, list) and len(period_dates) > 0:
                    print(f"    First 3: {period_dates[:3]}")
                    print(f"    Last 3: {period_dates[-3:]}")
                break
        
        # Check operating income
        oi = data.get("operating_income", [])
        print(f"  Operating income: {len(oi) if isinstance(oi, list) else 'N/A'} entries")
        if isinstance(oi, list) and len(oi) > 0:
            non_none = [x for x in oi if x is not None]
            print(f"    Non-None values: {len(non_none)}")
            if non_none:
                print(f"    First non-None: {non_none[0]}")
        
        # Check PPE
        ppe = data.get("ppe_net", [])
        print(f"  PPE net: {len(ppe) if isinstance(ppe, list) else 'N/A'} entries")
        if isinstance(ppe, list) and len(ppe) > 0:
            non_none = [x for x in ppe if x is not None]
            print(f"    Non-None values: {len(non_none)}")
            if non_none:
                print(f"    First non-None: {non_none[0]}")
        
        # Check market cap
        mc = data.get("market_cap", [])
        print(f"  Market cap: {len(mc) if isinstance(mc, list) else 'N/A'} entries")
        if isinstance(mc, list) and len(mc) > 0:
            non_none = [x for x in mc if x is not None and x > 0]
            print(f"    Positive values: {len(non_none)}")
            if non_none:
                print(f"    First positive: {non_none[0]}")
    else:
        print(f"\n{ticker}: NOT FOUND")

