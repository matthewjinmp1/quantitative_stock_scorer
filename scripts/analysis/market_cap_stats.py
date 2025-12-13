"""
Program to calculate median market cap for NYSE and NASDAQ stocks across all data points.
Extracts all market cap values from all quarters for all stocks and calculates the median for each exchange.
Also shows the top 10 market cap instances with ticker, company, and date.
"""
import json
import os
from typing import Dict, List, Tuple

def load_data_from_jsonl(filename: str) -> List[Dict]:
    """
    Load stock data from JSONL file (one JSON object per line)
    
    Args:
        filename: Path to JSONL file
    
    Returns:
        List of dictionaries containing stock data
    """
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found")
        return []
    
    stocks = []
    try:
        with open(filename, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                try:
                    stock = json.loads(line)
                    stocks.append(stock)
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON on line {line_num} in {filename}: {e}")
                    continue
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    return stocks

def extract_all_market_caps_with_info(stocks: List[Dict], exchange_name: str) -> Tuple[List[float], List[Tuple[float, str, str, str]]]:
    """
    Extract all market cap values from all stocks and all quarters, with associated information
    
    Args:
        stocks: List of stock data dictionaries
        exchange_name: Name of the exchange (for display purposes)
    
    Returns:
        Tuple of (list of market cap values, list of tuples with (market_cap, symbol, company_name, period))
    """
    all_market_caps = []
    market_caps_with_info = []
    
    print(f"Processing {len(stocks)} {exchange_name} stocks...")
    
    for stock_data in stocks:
        if not stock_data or "data" not in stock_data:
            continue
        
        symbol = stock_data.get("symbol", "N/A")
        company_name = stock_data.get("company_name", symbol)
        data = stock_data.get("data", {})
        
        # Get period dates
        period_dates = None
        for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
            if date_key in data and data[date_key]:
                period_dates = data[date_key]
                break
        
        if not period_dates or not isinstance(period_dates, list):
            continue
        
        market_caps = data.get("market_cap", [])
        
        # Ensure it's a list
        if not isinstance(market_caps, list):
            continue
        
        # Extract all non-None market cap values with their associated info
        for idx, market_cap in enumerate(market_caps):
            if market_cap is not None:
                try:
                    market_cap_float = float(market_cap)
                    if market_cap_float > 0:  # Only include positive values
                        all_market_caps.append(market_cap_float)
                        # Get the period for this index
                        period = period_dates[idx] if idx < len(period_dates) else "N/A"
                        market_caps_with_info.append((market_cap_float, symbol, company_name, period))
                except (ValueError, TypeError):
                    continue
    
    return all_market_caps, market_caps_with_info

def calculate_median(values: List[float]) -> float:
    """
    Calculate median of a list of values
    
    Args:
        values: List of numeric values
    
    Returns:
        Median value
    """
    if not values:
        return 0.0
    
    sorted_values = sorted(values)
    n = len(sorted_values)
    
    if n % 2 == 0:
        # Even number of values: average of two middle values
        median = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2.0
    else:
        # Odd number of values: middle value
        median = sorted_values[n // 2]
    
    return median

def format_number(num: float) -> str:
    """
    Format a number with appropriate units (millions, billions, etc.)
    
    Args:
        num: Number to format
    
    Returns:
        Formatted string
    """
    if num >= 1_000_000_000_000:
        return f"${num / 1_000_000_000_000:.2f}T"
    elif num >= 1_000_000_000:
        return f"${num / 1_000_000_000:.2f}B"
    elif num >= 1_000_000:
        return f"${num / 1_000_000:.2f}M"
    elif num >= 1_000:
        return f"${num / 1_000:.2f}K"
    else:
        return f"${num:.2f}"

def main():
    """
    Main function to calculate and display median market cap for NYSE and NASDAQ
    """
    print("=" * 80)
    print("Market Cap Statistics - NYSE and NASDAQ")
    print("=" * 80)
    
    # Load data from both exchanges
    print("\nLoading data from nyse_data.jsonl...")
    nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
    print(f"Found {len(nyse_stocks)} stock(s) in nyse_data.jsonl")
    
    print("\nLoading data from nasdaq_data.jsonl...")
    nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
    print(f"Found {len(nasdaq_stocks)} stock(s) in nasdaq_data.jsonl")
    
    if not nyse_stocks and not nasdaq_stocks:
        print("\nNo stock data found in either file")
        return
    
    # Extract all market caps with info
    print("\nExtracting market cap values...")
    nyse_market_caps, nyse_market_caps_info = extract_all_market_caps_with_info(nyse_stocks, "NYSE")
    nasdaq_market_caps, nasdaq_market_caps_info = extract_all_market_caps_with_info(nasdaq_stocks, "NASDAQ")
    
    # Calculate statistics
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)
    
    if nyse_market_caps:
        nyse_median = calculate_median(nyse_market_caps)
        nyse_min = min(nyse_market_caps)
        nyse_max = max(nyse_market_caps)
        nyse_mean = sum(nyse_market_caps) / len(nyse_market_caps)
        
        print(f"\nNYSE Market Cap Statistics:")
        print(f"  Total data points: {len(nyse_market_caps):,}")
        print(f"  Median: {format_number(nyse_median)} ({nyse_median:,.0f})")
        print(f"  Mean: {format_number(nyse_mean)} ({nyse_mean:,.0f})")
        print(f"  Minimum: {format_number(nyse_min)} ({nyse_min:,.0f})")
        print(f"  Maximum: {format_number(nyse_max)} ({nyse_max:,.0f})")
    else:
        print(f"\nNYSE: No market cap data found")
    
    if nasdaq_market_caps:
        nasdaq_median = calculate_median(nasdaq_market_caps)
        nasdaq_min = min(nasdaq_market_caps)
        nasdaq_max = max(nasdaq_market_caps)
        nasdaq_mean = sum(nasdaq_market_caps) / len(nasdaq_market_caps)
        
        print(f"\nNASDAQ Market Cap Statistics:")
        print(f"  Total data points: {len(nasdaq_market_caps):,}")
        print(f"  Median: {format_number(nasdaq_median)} ({nasdaq_median:,.0f})")
        print(f"  Mean: {format_number(nasdaq_mean)} ({nasdaq_mean:,.0f})")
        print(f"  Minimum: {format_number(nasdaq_min)} ({nasdaq_min:,.0f})")
        print(f"  Maximum: {format_number(nasdaq_max)} ({nasdaq_max:,.0f})")
    else:
        print(f"\nNASDAQ: No market cap data found")
    
    # Combined statistics
    all_market_caps_info = nyse_market_caps_info + nasdaq_market_caps_info
    if all_market_caps_info:
        all_market_caps = nyse_market_caps + nasdaq_market_caps
        combined_median = calculate_median(all_market_caps)
        combined_mean = sum(all_market_caps) / len(all_market_caps)
        
        print(f"\nCombined (NYSE + NASDAQ) Market Cap Statistics:")
        print(f"  Total data points: {len(all_market_caps):,}")
        print(f"  Median: {format_number(combined_median)} ({combined_median:,.0f})")
        print(f"  Mean: {format_number(combined_mean)} ({combined_mean:,.0f})")
        
        # Sort by market cap (descending) and get top 10
        all_market_caps_info_sorted = sorted(all_market_caps_info, key=lambda x: x[0], reverse=True)
        top_10 = all_market_caps_info_sorted[:10]
        
        print(f"\n{'=' * 80}")
        print("Top 10 Market Cap Instances (All Exchanges)")
        print(f"{'=' * 80}")
        print(f"{'Rank':<6} {'Market Cap':<20} {'Ticker':<10} {'Company Name':<40} {'Date':<15}")
        print(f"{'-' * 80}")
        
        for rank, (market_cap, symbol, company_name, period) in enumerate(top_10, start=1):
            # Truncate company name if too long
            display_company = company_name if len(company_name) <= 38 else company_name[:35] + "..."
            market_cap_str = format_number(market_cap)
            print(f"{rank:<6} {market_cap_str:<20} {symbol:<10} {display_company:<40} {period:<15}")
        
        print(f"{'=' * 80}")
    
    print()

if __name__ == "__main__":
    main()

