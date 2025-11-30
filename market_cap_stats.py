"""
Program to calculate median market cap for NYSE and NASDAQ stocks across all data points.
Extracts all market cap values from all quarters for all stocks and calculates the median for each exchange.
"""
import json
import os
from typing import Dict, List

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

def extract_all_market_caps(stocks: List[Dict], exchange_name: str) -> List[float]:
    """
    Extract all market cap values from all stocks and all quarters
    
    Args:
        stocks: List of stock data dictionaries
        exchange_name: Name of the exchange (for display purposes)
    
    Returns:
        List of all market cap values (floats)
    """
    all_market_caps = []
    
    print(f"Processing {len(stocks)} {exchange_name} stocks...")
    
    for stock_data in stocks:
        if not stock_data or "data" not in stock_data:
            continue
        
        data = stock_data.get("data", {})
        market_caps = data.get("market_cap", [])
        
        # Ensure it's a list
        if not isinstance(market_caps, list):
            continue
        
        # Extract all non-None market cap values
        for market_cap in market_caps:
            if market_cap is not None:
                try:
                    market_cap_float = float(market_cap)
                    if market_cap_float > 0:  # Only include positive values
                        all_market_caps.append(market_cap_float)
                except (ValueError, TypeError):
                    continue
    
    return all_market_caps

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
    
    # Extract all market caps
    print("\nExtracting market cap values...")
    nyse_market_caps = extract_all_market_caps(nyse_stocks, "NYSE")
    nasdaq_market_caps = extract_all_market_caps(nasdaq_stocks, "NASDAQ")
    
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
    if nyse_market_caps and nasdaq_market_caps:
        all_market_caps = nyse_market_caps + nasdaq_market_caps
        combined_median = calculate_median(all_market_caps)
        combined_mean = sum(all_market_caps) / len(all_market_caps)
        
        print(f"\nCombined (NYSE + NASDAQ) Market Cap Statistics:")
        print(f"  Total data points: {len(all_market_caps):,}")
        print(f"  Median: {format_number(combined_median)} ({combined_median:,.0f})")
        print(f"  Mean: {format_number(combined_mean)} ({combined_mean:,.0f})")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()

