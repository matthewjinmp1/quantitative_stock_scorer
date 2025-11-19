"""
Program to fetch quarterly financial data from QuickFS API for stocks listed
in tickers.json. Uses concurrent threading to fetch multiple tickers in parallel,
appends data to JSON incrementally as each ticker is fetched, and skips already
processed tickers to support resumable execution.
"""
import json
import os
import threading
import time
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
    Extract raw quarterly data from QuickFS response in compact format
    Stores data in QuickFS format: each metric is a key with a list of values
    
    Args:
        data: Full data response from QuickFS
        symbol: Original ticker symbol
    
    Returns:
        Dictionary containing raw quarterly data in QuickFS format
    """
    if not data or "financials" not in data:
        return None
    
    quarterly = data["financials"].get("quarterly")
    if not quarterly:
        return None
    
    # Get company name from metadata
    metadata = data.get("metadata", {})
    company_name = metadata.get("name", symbol)
    
    # Store quarterly data in QuickFS format (compact: metric -> list of values)
    return {
        "symbol": symbol,
        "company_name": company_name,
        "data": quarterly
    }

def fetch_single_ticker(ticker: str, max_retries: int = 3) -> Optional[Dict]:
    """
    Fetch data for a single ticker with rate limit handling
    
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
            error_str = str(e).lower()
            # Check for rate limit errors (429 or rate limit in message)
            is_rate_limit = '429' in error_str or 'rate limit' in error_str or 'too many requests' in error_str
            
            if is_rate_limit and attempt < max_retries - 1:
                # Exponential backoff for rate limits: 2^attempt seconds
                wait_time = 2 ** attempt
                print(f"  Rate limit hit for {ticker}, waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            elif attempt < max_retries - 1:
                # Short delay for other errors
                print(f"  Retry {attempt + 1}/{max_retries} for {ticker}...")
                time.sleep(1)
            else:
                print(f"  Error fetching {ticker} after {max_retries} attempts: {e}")
                return None
    
    return None

def fetch_all_tickers_individual(tickers: List[str], max_workers: int = 25, 
                                  output_file: str = "data.jsonl") -> List[Optional[Dict]]:
    """
    Fetch data for all tickers one at a time, sequentially (no threading)
    Appends data to JSON file as each ticker is fetched
    
    Args:
        tickers: List of stock ticker symbols
        max_workers: Ignored (kept for compatibility, but not used)
        output_file: Output JSON filename
    
    Returns:
        List of dictionaries containing quarterly data for each ticker
    """
    # Deduplicate input ticker list first (keep first occurrence)
    seen_input = set()
    unique_tickers = []
    for ticker in tickers:
        if ticker not in seen_input:
            unique_tickers.append(ticker)
            seen_input.add(ticker)
    
    if len(unique_tickers) < len(tickers):
        print(f"Removed {len(tickers) - len(unique_tickers)} duplicate ticker(s) from input list")
    
    # Load existing data to skip already processed tickers
    existing_data, processed_tickers = load_existing_data(output_file)
    if processed_tickers:
        print(f"Found {len(processed_tickers)} already processed ticker(s) in {output_file}, will skip...")
    
    # Filter out already processed tickers (those already in the file)
    remaining_tickers = [t for t in unique_tickers if t not in processed_tickers]
    
    if len(remaining_tickers) < len(unique_tickers):
        skipped_count = len(unique_tickers) - len(remaining_tickers)
        print(f"Skipping {skipped_count} ticker(s) that are already in {output_file}")
    
    total_tickers = len(unique_tickers)
    print(f"Fetching data for {len(remaining_tickers)} ticker(s) sequentially (one at a time)...")
    print(f"Data will be appended to {output_file} as fetched\n")
    remaining_indices = [i for i, t in enumerate(tickers) if t not in processed_tickers]
    
    if not remaining_tickers:
        print("All tickers already processed!")
        return []
    
    results = []
    completed = len(processed_tickers)
    
    # Fetch one ticker at a time sequentially
    for ticker in remaining_tickers:
        try:
            result = fetch_single_ticker(ticker)
            results.append(result)
            completed += 1
            
            if result:
                # Append to JSON file immediately
                append_stock_to_json(result, output_file, None)
                print(f"  [{completed}/{total_tickers}] Fetched and saved {ticker}")
            else:
                print(f"  [{completed}/{total_tickers}] Failed {ticker}")
        except Exception as e:
            print(f"  [{completed + 1}/{total_tickers}] Error processing {ticker}: {e}")
            results.append(None)
            completed += 1
    
    return results

def load_existing_data(filename: str = "data.jsonl") -> Tuple[List[Dict], Set[str]]:
    """
    Load existing data from JSONL file (one JSON object per line) and return set of already processed tickers
    Safely handles incomplete or corrupted lines (e.g., from interrupted writes)
    
    Args:
        filename: Path to JSONL file
    
    Returns:
        Tuple of (existing_data, processed_tickers_set)
    """
    if not os.path.exists(filename):
        return [], set()
    
    try:
        existing_data = []
        processed_tickers = set()
        valid_lines = []
        
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                try:
                    stock = json.loads(line)
                    symbol = stock.get("symbol")
                    if symbol:
                        processed_tickers.add(symbol)
                        existing_data.append(stock)
                        valid_lines.append(line)
                except json.JSONDecodeError:
                    # Skip invalid JSON lines (could be incomplete from interrupted write)
                    continue
        
        # If we found any invalid lines, optionally clean up the file
        # by rewriting only valid lines (but only if we're sure the file is corrupted)
        # For now, we just skip invalid lines when reading
        
        return existing_data, processed_tickers
    except (FileNotFoundError, IOError) as e:
        print(f"Warning: Error reading {filename}: {e}")
        return [], set()
    except Exception as e:
        print(f"Warning: Unexpected error reading {filename}: {e}")
        return [], set()

def format_stock_data_for_json(stock_data: Dict) -> Dict:
    """
    Format a single stock's data for JSON output (raw data in QuickFS format)
    
    Args:
        stock_data: Dictionary containing quarterly data for a stock
    
    Returns:
        Formatted dictionary for JSON output with all raw data in compact format
    """
    output_stock = {
        "symbol": stock_data.get("symbol"),
        "company_name": stock_data.get("company_name"),
        "data": stock_data.get("data", {})  # Store in QuickFS format: metric -> list of values
    }
    
    return output_stock

def append_stock_to_json(stock_data: Dict, filename: str = "data.jsonl", file_lock: threading.Lock = None):
    """
    Append a single stock's data to JSONL file (one JSON object per line)
    If ticker already exists, replaces it by rewriting the file atomically
    For new tickers, efficiently appends to end of file
    Uses atomic writes to prevent corruption if interrupted
    
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
            symbol = stock_data.get("symbol")
            formatted_data = format_stock_data_for_json(stock_data)
            new_line = json.dumps(formatted_data, separators=(',', ':'))
            
            # Quick check if ticker exists (only read first part of file if needed)
            ticker_found = False
            if os.path.exists(filename):
                # Check if ticker exists by scanning file
                with open(filename, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            stock = json.loads(line)
                            if stock.get("symbol") == symbol:
                                ticker_found = True
                                break
                        except json.JSONDecodeError:
                            continue
            
            if ticker_found:
                # Ticker exists - need to rewrite file to replace it
                # Use atomic write: write to temp file, then rename
                existing_lines = []
                if os.path.exists(filename):
                    with open(filename, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                stock = json.loads(line)
                                if stock.get("symbol") == symbol:
                                    # Replace with new data
                                    existing_lines.append(new_line)
                                else:
                                    existing_lines.append(line)
                            except json.JSONDecodeError:
                                # Skip invalid JSON lines to keep file clean
                                continue
                
                # Atomic write: write to temp file first, then rename
                temp_filename = filename + '.tmp'
                try:
                    with open(temp_filename, 'w') as f:
                        for line in existing_lines:
                            f.write(line + '\n')
                    # Only rename if write was successful
                    # On Windows, we need to remove the old file first if it exists
                    if os.path.exists(filename):
                        os.replace(temp_filename, filename)
                    else:
                        os.rename(temp_filename, filename)
                except Exception as e:
                    # If something went wrong, try to clean up temp file
                    if os.path.exists(temp_filename):
                        try:
                            os.remove(temp_filename)
                        except:
                            pass
                    raise e
            else:
                # Ticker doesn't exist - append new line
                # For JSONL format, appending is safe because each line is independent
                # If interrupted, only the last line might be incomplete, which we handle when reading
                try:
                    with open(filename, 'a') as f:
                        f.write(new_line + '\n')
                        f.flush()  # Ensure data is written to disk
                        os.fsync(f.fileno())  # Force write to disk (if available)
                except (AttributeError, OSError):
                    # os.fsync might not be available on all systems, that's okay
                    # The flush() is usually sufficient
                    with open(filename, 'a') as f:
                        f.write(new_line + '\n')
                        f.flush()
        finally:
            if file_lock:
                file_lock.release()
                
    except Exception as e:
        print(f"  Error appending {stock_data.get('symbol', 'unknown')} to {filename}: {e}")

def save_to_json(all_data: List[Dict], filename: str = "data.jsonl"):
    """
    Save all quarterly data to JSONL file (one JSON object per line)
    
    Args:
        all_data: List of dictionaries containing quarterly data for all stocks
        filename: Output filename
    """
    try:
        # Write each stock as a separate line in JSONL format
        with open(filename, 'w') as f:
            for stock_data in all_data:
                if stock_data:  # Only include valid data
                    formatted_data = format_stock_data_for_json(stock_data)
                    f.write(json.dumps(formatted_data, separators=(',', ':')) + '\n')
        
        count = sum(1 for stock_data in all_data if stock_data)
        print(f"\nFinal data saved to {filename}")
        print(f"Saved data for {count} stock(s)")
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
    tickers = load_tickers("nyse.json")
    if not tickers:
        print("No tickers found. Please check nyse.json")
        return
    
    print(f"\nFound {len(tickers)} ticker(s)\n")
    
    # Fetch data for all tickers individually (data is appended as fetched)
    # Fetching sequentially (one at a time) to measure baseline speed
    all_results = fetch_all_tickers_individual(tickers, max_workers=1, output_file="data.jsonl")
    
    # Filter out None results for summary
    all_data = [stock_data for stock_data in all_results if stock_data]
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Successfully fetched data for {len(all_data)} stock(s) out of {len(tickers)}")
    if len(all_data) < len(tickers):
        print(f"Failed to fetch data for {len(tickers) - len(all_data)} stock(s)")
    print(f"\nAll data has been saved to data.jsonl")

if __name__ == "__main__":
    main()

