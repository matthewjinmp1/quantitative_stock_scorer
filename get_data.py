"""
Program to fetch quarterly price and dividend data from QuickFS API
for stocks listed in tickers.json, fetching one ticker at a time
and saving all data to JSON
"""
import json
import os
import threading
from quickfs import QuickFS
from typing import Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import QUICKFS_API_KEY

# QuickFS API Configuration
API_KEY = QUICKFS_API_KEY

def load_tickers(filename: str = "tickers.json") -> List[str]:
    """
    Load ticker symbols from JSON file
    
    Args:
        filename: Path to JSON file containing tickers
    
    Returns:
        List of ticker symbols
    """
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            return data.get("tickers", [])
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filename}")
        return []

def format_symbol(ticker: str) -> str:
    """
    Format ticker symbol for QuickFS (add :US for US stocks)
    
    Args:
        ticker: Ticker symbol
    
    Returns:
        Formatted ticker symbol
    """
    if ":" not in ticker:
        return f"{ticker}:US"
    return ticker

def process_quarterly_data(data: Dict, symbol: str) -> Optional[Dict]:
    """
    Extract raw quarterly data from QuickFS response without any calculations
    
    Args:
        data: Full data response from QuickFS
        symbol: Original ticker symbol
    
    Returns:
        Dictionary containing raw quarterly data
    """
    if not data or "financials" not in data:
        return None
    
    quarterly = data["financials"].get("quarterly")
    if not quarterly:
        return None
    
    # Get company name from metadata
    metadata = data.get("metadata", {})
    company_name = metadata.get("name", symbol)
    
    # Get all available quarterly metrics - store everything as raw data
    # Find the longest list to determine number of periods
    all_keys = list(quarterly.keys())
    if not all_keys:
        return None
    
    # Find the maximum length to determine number of periods
    # Only consider keys that have list values
    lengths = []
    for key in all_keys:
        value = quarterly.get(key)
        if isinstance(value, list):
            lengths.append(len(value))
    
    if not lengths:
        return None
    
    max_length = max(lengths)
    
    # Build raw data entries for each period
    quarterly_data = []
    for j in range(max_length):
        period_entry = {}
        
        # Store all available metrics for this period
        for key in all_keys:
            values = quarterly.get(key)
            # Only process if it's a list
            if isinstance(values, list):
                if j < len(values):
                    value = values[j]
                    # Store the value as-is (could be None, number, string, etc.)
                    period_entry[key] = value
                else:
                    # If this metric doesn't have data for this period, set to None
                    period_entry[key] = None
            else:
                # If the value is not a list (e.g., a single value), store it for all periods
                # or set to None if it's not applicable
                period_entry[key] = values
        
        quarterly_data.append(period_entry)
    
    return {
        "symbol": symbol,
        "company_name": company_name,
        "data": quarterly_data
    }

def fetch_single_ticker(ticker: str, max_retries: int = 3) -> Optional[Dict]:
    """
    Fetch data for a single ticker
    
    Args:
        ticker: Stock ticker symbol (e.g., 'GOOGL')
        max_retries: Maximum number of retry attempts
    
    Returns:
        Dictionary containing quarterly data or None
    """
    formatted_symbol = format_symbol(ticker)
    
    for attempt in range(max_retries):
        try:
            client = QuickFS(API_KEY)
            data = client.get_data_full(formatted_symbol)
            processed_data = process_quarterly_data(data, ticker)
            return processed_data
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Retry {attempt + 1}/{max_retries} for {ticker}...")
            else:
                print(f"  Error fetching {ticker} after {max_retries} attempts: {e}")
                return None
    
    return None

def fetch_all_tickers_individual(tickers: List[str], max_workers: int = 10, 
                                  output_file: str = "data.json") -> List[Optional[Dict]]:
    """
    Fetch data for all tickers one at a time, using concurrent requests
    Appends data to JSON file as each ticker is fetched
    
    Args:
        tickers: List of stock ticker symbols
        max_workers: Maximum number of concurrent threads (default: 10)
        output_file: Output JSON filename
    
    Returns:
        List of dictionaries containing quarterly data for each ticker
    """
    total_tickers = len(tickers)
    print(f"Fetching data for {total_tickers} ticker(s) individually...")
    print(f"Using {max_workers} concurrent threads")
    print(f"Data will be appended to {output_file} as fetched\n")
    
    # Load existing data to skip already processed tickers
    existing_data, processed_tickers = load_existing_data(output_file)
    if processed_tickers:
        print(f"Found {len(processed_tickers)} already processed ticker(s), skipping...")
    
    # Filter out already processed tickers
    remaining_tickers = [t for t in tickers if t not in processed_tickers]
    remaining_indices = [i for i, t in enumerate(tickers) if t not in processed_tickers]
    
    if not remaining_tickers:
        print("All tickers already processed!")
        return [None] * total_tickers
    
    print(f"Fetching {len(remaining_tickers)} remaining ticker(s)...\n")
    
    results = [None] * total_tickers
    completed = len(processed_tickers)
    
    # File lock for thread-safe JSON writing
    file_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks for remaining tickers
        future_to_info = {}
        for idx, ticker in enumerate(remaining_tickers):
            original_index = remaining_indices[idx]
            future = executor.submit(fetch_single_ticker, ticker)
            future_to_info[future] = (original_index, ticker)
        
        # Process completed tasks
        for future in as_completed(future_to_info):
            original_index, ticker = future_to_info[future]
            try:
                result = future.result()
                results[original_index] = result
                completed += 1
                
                if result:
                    # Append to JSON file immediately
                    append_stock_to_json(result, output_file, file_lock)
                    print(f"  [{completed}/{total_tickers}] Fetched and saved {ticker}")
                else:
                    print(f"  [{completed}/{total_tickers}] Failed {ticker}")
            except Exception as e:
                print(f"  [{completed + 1}/{total_tickers}] Error processing {ticker}: {e}")
                completed += 1
    
    return results

def load_existing_data(filename: str = "data.json") -> Tuple[List[Dict], Set[str]]:
    """
    Load existing data from JSON file and return set of already processed tickers
    
    Args:
        filename: Path to JSON file
    
    Returns:
        Tuple of (existing_data, processed_tickers_set)
    """
    if not os.path.exists(filename):
        return [], set()
    
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                processed_tickers = {stock.get("symbol") for stock in data if stock.get("symbol")}
                return data, processed_tickers
            return [], set()
    except (json.JSONDecodeError, FileNotFoundError):
        return [], set()

def format_stock_data_for_json(stock_data: Dict) -> Dict:
    """
    Format a single stock's data for JSON output (raw data, no calculations)
    
    Args:
        stock_data: Dictionary containing quarterly data for a stock
    
    Returns:
        Formatted dictionary for JSON output with all raw data
    """
    output_stock = {
        "symbol": stock_data.get("symbol"),
        "company_name": stock_data.get("company_name"),
        "data": stock_data.get("data", [])  # Store all raw data as-is
    }
    
    return output_stock

def append_stock_to_json(stock_data: Dict, filename: str = "data.json", file_lock: threading.Lock = None):
    """
    Append a single stock's data to JSON file
    
    Args:
        stock_data: Dictionary containing quarterly data for a stock
        filename: Output filename
        file_lock: Thread lock for file operations (optional)
    """
    if not stock_data:
        return
    
    try:
        # Use lock if provided (for thread safety)
        if file_lock:
            file_lock.acquire()
        
        try:
            # Load existing data
            existing_data, _ = load_existing_data(filename)
            
            # Check if this ticker already exists
            symbol = stock_data.get("symbol")
            existing_data = [s for s in existing_data if s.get("symbol") != symbol]
            
            # Add new stock data
            formatted_data = format_stock_data_for_json(stock_data)
            existing_data.append(formatted_data)
            
            # Write back to file
            with open(filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
        finally:
            if file_lock:
                file_lock.release()
                
    except Exception as e:
        print(f"  Error appending {stock_data.get('symbol', 'unknown')} to {filename}: {e}")

def save_to_json(all_data: List[Dict], filename: str = "data.json"):
    """
    Save all quarterly data to JSON file (for final save/update)
    
    Args:
        all_data: List of dictionaries containing quarterly data for all stocks
        filename: Output filename
    """
    try:
        # Create output data with period, total_return, forward_return, and roa
        output_data = []
        for stock_data in all_data:
            if stock_data:  # Only include valid data
                formatted_data = format_stock_data_for_json(stock_data)
                output_data.append(formatted_data)
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nFinal data saved to {filename}")
        print(f"Saved data for {len(output_data)} stock(s)")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def main():
    """
    Main function to fetch quarterly data for all tickers individually
    and save to JSON
    """
    print("Fetching Quarterly Price and Dividend Data (Individual Requests)")
    print("=" * 80)
    
    # Load tickers
    tickers = load_tickers("tickers.json")
    if not tickers:
        print("No tickers found. Please check tickers.json")
        return
    
    print(f"\nFound {len(tickers)} ticker(s)\n")
    
    # Fetch data for all tickers individually (data is appended as fetched)
    all_results = fetch_all_tickers_individual(tickers, max_workers=10, output_file="data.json")
    
    # Filter out None results for summary
    all_data = [stock_data for stock_data in all_results if stock_data]
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Successfully fetched data for {len(all_data)} stock(s) out of {len(tickers)}")
    if len(all_data) < len(tickers):
        print(f"Failed to fetch data for {len(tickers) - len(all_data)} stock(s)")
    print(f"\nAll data has been saved to data.json")

if __name__ == "__main__":
    main()

