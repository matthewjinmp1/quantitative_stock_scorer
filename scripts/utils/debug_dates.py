import json

# Check a sample stock to see date formats
with open('nyse_data.jsonl', 'r') as f:
    for i, line in enumerate(f):
        if i >= 3:
            break
        stock = json.loads(line)
        symbol = stock.get('symbol', 'N/A')
        data = stock.get('data', {})
        
        # Check different date fields
        for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
            if date_key in data:
                dates = data[date_key]
                if isinstance(dates, list) and len(dates) > 0:
                    print(f"\n{symbol} - {date_key}:")
                    print(f"  First 5 dates: {dates[:5]}")
                    print(f"  Last 5 dates: {dates[-5:]}")
                    break

