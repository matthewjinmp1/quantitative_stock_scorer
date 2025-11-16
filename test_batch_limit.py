"""Test QuickFS batch request limits"""
from quickfs import QuickFS
from config import QUICKFS_API_KEY
import json

client = QuickFS(QUICKFS_API_KEY)

# Load tickers
with open('tickers.json', 'r') as f:
    tickers_data = json.load(f)
    all_tickers = tickers_data['tickers']

print(f"Total tickers available: {len(all_tickers)}")

# Test with different batch sizes
test_sizes = [10, 50, 100, 500, 1000, 2000, 3600]

metrics = ['period_end_price', 'dividends', 'period_end_date', 'roa']
period = 'FQ-10:FQ'  # Use fewer periods for testing

for size in test_sizes:
    if size > len(all_tickers):
        print(f"\nSkipping {size} (exceeds available tickers)")
        continue
    
    print(f"\nTesting batch size: {size}")
    test_tickers = [f"{t}:US" for t in all_tickers[:size]]
    
    try:
        result = client.get_data_batch(test_tickers, metrics, period)
        
        # Count successful tickers
        if result and 'period_end_date' in result:
            successful = len([k for k in result['period_end_date'].keys()])
            print(f"  Success! Got data for {successful} tickers")
        else:
            print(f"  Warning: Unexpected response format")
            
    except Exception as e:
        print(f"  Error: {e}")
        print(f"  Maximum batch size appears to be less than {size}")
        break

