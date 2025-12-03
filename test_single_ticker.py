import json
from ebit_ppe_portfolio_backtest import get_ebit_ppe_at_date, get_market_cap_at_date, load_data_from_jsonl, get_period_dates, parse_date, find_quarter_near_date

# Load a few stocks
nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
all_stocks = nyse_stocks + nasdaq_stocks

print(f"Loaded {len(all_stocks)} stocks")

# Test with first few stocks
for stock in all_stocks[:10]:
    symbol = stock.get("symbol", "N/A")
    print(f"\n{symbol}:")
    
    data = stock.get("data", {})
    period_dates = get_period_dates(data)
    print(f"  Period dates: {len(period_dates) if period_dates else 0}")
    if period_dates and len(period_dates) > 0:
        print(f"    First: {period_dates[0]}, Last: {period_dates[-1]}")
        first_date = parse_date(period_dates[0])
        if first_date:
            print(f"    First year: {first_date.year}")
    
    # Test EBIT/PPE
    ebit_result = get_ebit_ppe_at_date(stock, 2000)
    print(f"  EBIT/PPE for 2000: {ebit_result}")
    
    # Test market cap
    mc_result = get_market_cap_at_date(stock, 2000)
    print(f"  Market cap for 2000: {mc_result}")
    
    if ebit_result and mc_result:
        print(f"  ✓ FOUND DATA!")
        break

