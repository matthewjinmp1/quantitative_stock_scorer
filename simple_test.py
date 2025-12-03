import json
from ebit_ppe_portfolio_backtest import get_ebit_ppe_at_date, get_market_cap_at_date

# Load one stock
with open('nyse_data.jsonl', 'r') as f:
    line = f.readline()
    if line.strip():
        stock = json.loads(line)
        symbol = stock.get("symbol", "N/A")
        print(f"Testing with: {symbol}")
        
        # Check data structure
        data = stock.get("data", {})
        print(f"Has data: {'data' in stock}")
        print(f"Has operating_income: {'operating_income' in data}")
        print(f"Has ppe_net: {'ppe_net' in data}")
        print(f"Has market_cap: {'market_cap' in data}")
        
        if 'operating_income' in data:
            oi = data['operating_income']
            print(f"Operating income type: {type(oi)}, length: {len(oi) if isinstance(oi, list) else 'N/A'}")
        
        # Test functions
        print("\nTesting get_ebit_ppe_at_date:")
        result = get_ebit_ppe_at_date(stock, 2000)
        print(f"Result: {result}")
        
        print("\nTesting get_market_cap_at_date:")
        result2 = get_market_cap_at_date(stock, 2000)
        print(f"Result: {result2}")

