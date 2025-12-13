import json
from ebit_ppe_portfolio_backtest import get_period_dates, parse_date, find_quarter_near_date

# Load one stock
with open('nyse_data.jsonl', 'r') as f:
    line = f.readline()
    if line.strip():
        stock = json.loads(line)
        data = stock.get("data", {})
        period_dates = get_period_dates(data)
        
        print(f"Period dates: {len(period_dates) if period_dates else 0}")
        if period_dates:
            print(f"First 10: {period_dates[:10]}")
            print(f"Last 10: {period_dates[-10:]}")
            
            # Test parsing
            print("\nTesting date parsing:")
            for date_str in period_dates[:5]:
                parsed = parse_date(date_str)
                print(f"  {date_str} -> {parsed} (year: {parsed.year if parsed else None})")
            
            # Test finding quarter
            print("\nTesting find_quarter_near_date for 2000:")
            idx = find_quarter_near_date(period_dates, 2000, allow_earlier=True)
            print(f"Found index: {idx}")
            if idx is not None:
                print(f"Date at index: {period_dates[idx]}")
                parsed = parse_date(period_dates[idx])
                print(f"Parsed date: {parsed}")

