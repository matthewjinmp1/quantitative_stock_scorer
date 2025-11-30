"""
Program to calculate EBIT/PPE scores and percentiles for stocks from both NYSE and NASDAQ data files.
Gets the most recent quarter's EBIT/PPE for each stock, ranks them, calculates percentiles, and saves to scores.json.
EBIT/PPE = Operating Income / PPE (Property, Plant, and Equipment)

Usage:
    python calculate_scores.py calc        - Calculate and save scores for all stocks
    python calculate_scores.py <symbol>     - Look up percentile rank for a specific stock (e.g., AAPL)
"""
import json
import os
import time
from typing import Dict, List, Optional, Tuple

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

def get_most_recent_ebit_ppe(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """
    Extract the most recent quarter's EBIT/PPE for a stock
    
    Args:
        stock_data: Dictionary containing stock data from JSONL file
    
    Returns:
        Tuple of (symbol, company_name, ebit_ppe_value, period) or None if not available
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    
    # Extract arrays from the data dictionary
    # Try different possible key names for dates
    period_dates = None
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            period_dates = data[date_key]
            break
    
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    ppe_net = data.get("ppe_net", [])
    
    # Ensure arrays are lists
    if not isinstance(operating_income, list):
        operating_income = []
    if not isinstance(ppe_net, list):
        ppe_net = []
    
    # Find the most recent quarter with valid EBIT/PPE data
    # Start from the most recent (last index) and work backwards
    for j in range(len(period_dates) - 1, -1, -1):
        if j < len(operating_income) and j < len(ppe_net):
            if operating_income[j] is not None and ppe_net[j] is not None:
                if ppe_net[j] != 0:
                    ebit_ppe = operating_income[j] / ppe_net[j]
                    period = period_dates[j]
                    return (symbol, company_name, ebit_ppe, period)
    
    return None

def calculate_percentile(rank: int, total: int) -> float:
    """
    Calculate percentile rank (0-100) for a given rank
    
    Args:
        rank: Rank of the stock (1-based, where 1 is the highest value)
        total: Total number of stocks
    
    Returns:
        Percentile (0-100), where 100 is the highest value
    """
    if total == 0:
        return 0.0
    if total == 1:
        return 100.0
    
    # Percentile formula: (total - rank + 1) / total * 100
    # This gives 100 to the highest value, 0 to the lowest
    percentile = (total - rank + 1) / total * 100.0
    return percentile

def calculate_scores_for_all_stocks(nyse_stocks: List[Dict], nasdaq_stocks: List[Dict]) -> List[Dict]:
    """
    Calculate EBIT/PPE scores and percentiles for all stocks from both exchanges
    
    Args:
        nyse_stocks: List of stock data dictionaries from NYSE JSONL file
        nasdaq_stocks: List of stock data dictionaries from NASDAQ JSONL file
    
    Returns:
        List of dictionaries containing symbol, company_name, ebit_ppe, period, rank, and percentile
    """
    all_ebit_ppe_data = []
    
    # Process NYSE stocks
    print(f"Processing {len(nyse_stocks)} NYSE stocks...")
    for stock_data in nyse_stocks:
        result = get_most_recent_ebit_ppe(stock_data)
        if result:
            symbol, company_name, ebit_ppe, period = result
            all_ebit_ppe_data.append({
                "symbol": symbol,
                "company_name": company_name,
                "ebit_ppe": ebit_ppe,
                "period": period,
                "exchange": "NYSE"
            })
    
    # Process NASDAQ stocks
    print(f"Processing {len(nasdaq_stocks)} NASDAQ stocks...")
    for stock_data in nasdaq_stocks:
        result = get_most_recent_ebit_ppe(stock_data)
        if result:
            symbol, company_name, ebit_ppe, period = result
            all_ebit_ppe_data.append({
                "symbol": symbol,
                "company_name": company_name,
                "ebit_ppe": ebit_ppe,
                "period": period,
                "exchange": "NASDAQ"
            })
    
    # Sort by EBIT/PPE in descending order (highest first)
    all_ebit_ppe_data.sort(key=lambda x: x["ebit_ppe"], reverse=True)
    
    # Add rank and percentile
    total_stocks = len(all_ebit_ppe_data)
    for rank, stock_data in enumerate(all_ebit_ppe_data, start=1):
        stock_data["rank"] = rank
        stock_data["percentile"] = calculate_percentile(rank, total_stocks)
    
    return all_ebit_ppe_data

def save_scores_to_json(scores_data: List[Dict], filename: str = "scores.json"):
    """
    Save calculated scores to JSON file
    
    Args:
        scores_data: List of dictionaries containing scores for each stock
        filename: Output filename
    """
    try:
        output_data = {
            "metadata": {
                "total_stocks": len(scores_data),
                "calculation_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "metric": "EBIT/PPE (most recent quarter)",
                "description": "EBIT/PPE = Operating Income / PPE. Percentile where 100 is highest EBIT/PPE, 0 is lowest."
            },
            "scores": scores_data
        }
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nScores saved to {filename}")
        print(f"Saved scores for {len(scores_data)} stock(s)")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def load_scores_from_json(filename: str = "scores.json") -> Optional[Dict]:
    """
    Load scores from JSON file
    
    Args:
        filename: Path to scores JSON file
    
    Returns:
        Dictionary containing scores data or None if file doesn't exist
    """
    if not os.path.exists(filename):
        return None
    
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return None

def lookup_stock(symbol: str, filename: str = "scores.json") -> Optional[Dict]:
    """
    Look up a stock's percentile rank by symbol
    
    Args:
        symbol: Stock symbol to look up (case-insensitive)
        filename: Path to scores JSON file
    
    Returns:
        Dictionary containing stock score data or None if not found
    """
    scores_data = load_scores_from_json(filename)
    if not scores_data:
        print(f"Error: {filename} not found. Please run 'calc' command first.")
        return None
    
    scores = scores_data.get("scores", [])
    symbol_upper = symbol.upper()
    
    for stock in scores:
        if stock.get("symbol", "").upper() == symbol_upper:
            return stock
    
    return None

def display_stock_info(stock: Dict):
    """
    Display stock percentile rank information
    
    Args:
        stock: Dictionary containing stock score data
    """
    print(f"\n{'='*80}")
    print(f"Stock: {stock['symbol']} - {stock['company_name']}")
    print(f"{'='*80}")
    print(f"Exchange: {stock.get('exchange', 'N/A')}")
    print(f"Period: {stock.get('period', 'N/A')}")
    print(f"EBIT/PPE: {stock['ebit_ppe']:.4f}")
    print(f"Rank: {stock['rank']:,} out of {stock.get('total_stocks', 'N/A')}")
    print(f"Percentile: {stock['percentile']:.2f}")
    print(f"{'='*80}\n")

def run_calculate_command():
    """
    Execute the 'calc' command to calculate and save scores for all stocks
    """
    program_start_time = time.time()
    print("Calculating EBIT/PPE Scores and Percentiles")
    print("=" * 80)
    
    # Load data from both exchanges
    print("\nLoading data from nyse_data.jsonl...")
    nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
    print(f"Found {len(nyse_stocks)} stock(s) in nyse_data.jsonl")
    
    print("\nLoading data from nasdaq_data.jsonl...")
    nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
    print(f"Found {len(nasdaq_stocks)} stock(s) in nasdaq_data.jsonl")
    
    if not nyse_stocks and not nasdaq_stocks:
        print("No stock data found in either file")
        total_time = time.time() - program_start_time
        print(f"\n{'='*80}")
        print(f"Total program execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        return
    
    # Calculate scores for all stocks
    print("\nCalculating EBIT/PPE scores and percentiles...")
    start_time = time.time()
    scores_data = calculate_scores_for_all_stocks(nyse_stocks, nasdaq_stocks)
    elapsed_time = time.time() - start_time
    print(f"Score calculation completed in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    
    if not scores_data:
        print("\nNo EBIT/PPE scores were successfully calculated.")
        total_time = time.time() - program_start_time
        print(f"\n{'='*80}")
        print(f"Total program execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
        print(f"{'='*80}")
        return
    
    # Save to scores.json
    save_scores_to_json(scores_data, "scores.json")
    
    # Print summary statistics
    print(f"\n{'='*80}")
    print("SCORING SUMMARY")
    print(f"{'='*80}")
    print(f"Total stocks with EBIT/PPE data: {len(scores_data)}")
    
    if scores_data:
        ebit_ppe_values = [s["ebit_ppe"] for s in scores_data]
        print(f"\nEBIT/PPE Statistics:")
        print(f"  Highest EBIT/PPE: {max(ebit_ppe_values):.4f} (Rank 1, Percentile 100.0)")
        print(f"  Lowest EBIT/PPE: {min(ebit_ppe_values):.4f} (Rank {len(scores_data)}, Percentile 0.0)")
        print(f"  Median EBIT/PPE: {sorted(ebit_ppe_values)[len(ebit_ppe_values)//2]:.4f}")
        print(f"  Mean EBIT/PPE: {sum(ebit_ppe_values)/len(ebit_ppe_values):.4f}")
        
        # Count by exchange
        nyse_count = sum(1 for s in scores_data if s.get("exchange") == "NYSE")
        nasdaq_count = sum(1 for s in scores_data if s.get("exchange") == "NASDAQ")
        print(f"\nBy Exchange:")
        print(f"  NYSE: {nyse_count} stocks")
        print(f"  NASDAQ: {nasdaq_count} stocks")
        
        # Show top 10
        print(f"\nTop 10 Stocks by EBIT/PPE:")
        for i, stock in enumerate(scores_data[:10], 1):
            print(f"  {i}. {stock['symbol']} ({stock['company_name']}): {stock['ebit_ppe']:.4f} (Percentile: {stock['percentile']:.2f})")
    
    total_time = time.time() - program_start_time
    print(f"{'='*80}")
    print(f"Total program execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"{'='*80}")

def run_lookup_command(symbol: str):
    """
    Execute the stock lookup command to display percentile rank for a specific stock
    
    Args:
        symbol: Stock symbol to look up
    """
    stock = lookup_stock(symbol)
    if stock:
        # Get total stocks count from metadata
        scores_data = load_scores_from_json()
        if scores_data:
            total_stocks = scores_data.get("metadata", {}).get("total_stocks", 0)
            stock["total_stocks"] = total_stocks
        display_stock_info(stock)
    else:
        print(f"\nStock '{symbol}' not found in scores.json")
        print("Make sure you've run 'calc' first, and that the stock symbol is correct.\n")

def print_help():
    """Print help message with available commands"""
    print("\n" + "=" * 80)
    print("Available Commands:")
    print("=" * 80)
    print("  calc                   - Calculate and save scores for all stocks")
    print("  <symbol>               - Look up percentile rank for a stock (e.g., AAPL, MSFT)")
    print("  help                   - Show this help message")
    print("  exit / quit            - Exit the program")
    print("=" * 80 + "\n")

def main():
    """
    Main function with interactive command terminal
    
    Commands:
        calc        - Calculate and save scores for all stocks
        <symbol>    - Look up percentile rank for a specific stock (e.g., AAPL)
        help        - Show help message
        exit/quit   - Exit the program
    """
    print("=" * 80)
    print("EBIT/PPE Stock Scorer - Interactive Terminal")
    print("=" * 80)
    print_help()
    
    while True:
        try:
            # Get user input
            user_input = input("Enter command: ").strip()
            
            if not user_input:
                continue
            
            command = user_input.lower()
            
            # Handle commands
            if command == "exit" or command == "quit":
                print("\nExiting program. Goodbye!\n")
                break
            elif command == "help":
                print_help()
            elif command == "calc":
                run_calculate_command()
                print()  # Add blank line after command
            else:
                # Treat as stock symbol lookup
                run_lookup_command(user_input)
        
        except KeyboardInterrupt:
            print("\n\nExiting program. Goodbye!\n")
            break
        except EOFError:
            print("\n\nExiting program. Goodbye!\n")
            break
        except Exception as e:
            print(f"\nError: {e}\n")

if __name__ == "__main__":
    main()

