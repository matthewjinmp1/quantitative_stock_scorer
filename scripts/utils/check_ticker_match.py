import json

# Load S&P 500 tickers
with open('sp500_2000.json', 'r') as f:
    sp500_data = json.load(f)
    sp500_tickers = [t.upper() for t in sp500_data.get("tickers", [])]

print(f"S&P 500 tickers: {len(sp500_tickers)}")
print(f"Sample: {sp500_tickers[:10]}")

# Load stock data and get all symbols
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
data_symbols = {s.get("symbol", "").upper() for s in all_stocks}

print(f"\nData file symbols: {len(data_symbols)}")
print(f"Sample: {list(data_symbols)[:10]}")

# Check overlap
sp500_set = set(sp500_tickers)
overlap = sp500_set & data_symbols

print(f"\nOverlap: {len(overlap)} tickers found in both")
print(f"Missing from data: {len(sp500_set - data_symbols)}")
print(f"Sample missing: {list(sp500_set - data_symbols)[:20]}")

# Check a specific ticker that should be there
test_tickers = ["AAPL", "MSFT", "JNJ", "WMT", "PG", "XOM", "CVX"]
print(f"\nChecking specific tickers:")
for ticker in test_tickers:
    in_sp500 = ticker in sp500_set
    in_data = ticker in data_symbols
    print(f"  {ticker}: SP500={in_sp500}, Data={in_data}")

