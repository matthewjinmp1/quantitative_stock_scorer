"""
Program to calculate EBIT/PPE and Operating Margin scores and percentiles for stocks from both NYSE and NASDAQ data files.
Gets the most recent quarter's EBIT/PPE and Operating Margin for each stock, ranks them, calculates percentiles, 
and creates a combined total percentile based on both metrics. Saves results to scores.json.

EBIT/PPE = Operating Income / PPE (Property, Plant, and Equipment)
Operating Margin = Operating Income / Revenue
Total Percentile = Combined percentile based on average rank of EBIT/PPE and Operating Margin

Usage:
    python scorer.py calc        - Calculate and save scores for all stocks
    python scorer.py <symbol>   - Look up percentile rank for a specific stock (e.g., AAPL)
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

def get_most_recent_operating_margin(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """
    Extract the most recent quarter's Operating Margin for a stock
    
    Args:
        stock_data: Dictionary containing stock data from JSONL file
    
    Returns:
        Tuple of (symbol, company_name, operating_margin_value, period) or None if not available
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
    revenue = data.get("revenue", [])
    
    # Ensure arrays are lists
    if not isinstance(operating_income, list):
        operating_income = []
    if not isinstance(revenue, list):
        revenue = []
    
    # Find the most recent quarter with valid Operating Margin data
    # Start from the most recent (last index) and work backwards
    # Operating Margin = Operating Income / Revenue
    for j in range(len(period_dates) - 1, -1, -1):
        if j < len(operating_income) and j < len(revenue):
            if operating_income[j] is not None and revenue[j] is not None:
                if revenue[j] != 0:
                    operating_margin = operating_income[j] / revenue[j]
                    period = period_dates[j]
                    return (symbol, company_name, operating_margin, period)
    
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
    Calculate EBIT/PPE and Operating Margin scores and percentiles for all stocks from both exchanges.
    Also calculates a combined total percentile based on both metrics.
    
    Args:
        nyse_stocks: List of stock data dictionaries from NYSE JSONL file
        nasdaq_stocks: List of stock data dictionaries from NASDAQ JSONL file
    
    Returns:
        List of dictionaries containing symbol, company_name, ebit_ppe, operating_margin, 
        ebit_ppe_rank, operating_margin_rank, ebit_ppe_percentile, operating_margin_percentile, 
        total_rank, total_percentile, period, and exchange
    """
    all_stock_data = []
    
    # Process NYSE stocks
    print(f"Processing {len(nyse_stocks)} NYSE stocks...")
    for stock_data in nyse_stocks:
        ebit_ppe_result = get_most_recent_ebit_ppe(stock_data)
        operating_margin_result = get_most_recent_operating_margin(stock_data)
        
        # Only include stocks that have at least one metric
        if ebit_ppe_result or operating_margin_result:
            symbol = stock_data.get("symbol")
            company_name = stock_data.get("company_name", symbol)
            
            stock_entry = {
                "symbol": symbol,
                "company_name": company_name,
                "exchange": "NYSE",
                "ebit_ppe": None,
                "operating_margin": None,
                "period": None
            }
            
            if ebit_ppe_result:
                _, _, ebit_ppe, period = ebit_ppe_result
                stock_entry["ebit_ppe"] = ebit_ppe
                if stock_entry["period"] is None:
                    stock_entry["period"] = period
            
            if operating_margin_result:
                _, _, operating_margin, period = operating_margin_result
                stock_entry["operating_margin"] = operating_margin
                if stock_entry["period"] is None:
                    stock_entry["period"] = period
            
            all_stock_data.append(stock_entry)
    
    # Process NASDAQ stocks
    print(f"Processing {len(nasdaq_stocks)} NASDAQ stocks...")
    for stock_data in nasdaq_stocks:
        ebit_ppe_result = get_most_recent_ebit_ppe(stock_data)
        operating_margin_result = get_most_recent_operating_margin(stock_data)
        
        # Only include stocks that have at least one metric
        if ebit_ppe_result or operating_margin_result:
            symbol = stock_data.get("symbol")
            company_name = stock_data.get("company_name", symbol)
            
            stock_entry = {
                "symbol": symbol,
                "company_name": company_name,
                "exchange": "NASDAQ",
                "ebit_ppe": None,
                "operating_margin": None,
                "period": None
            }
            
            if ebit_ppe_result:
                _, _, ebit_ppe, period = ebit_ppe_result
                stock_entry["ebit_ppe"] = ebit_ppe
                if stock_entry["period"] is None:
                    stock_entry["period"] = period
            
            if operating_margin_result:
                _, _, operating_margin, period = operating_margin_result
                stock_entry["operating_margin"] = operating_margin
                if stock_entry["period"] is None:
                    stock_entry["period"] = period
            
            all_stock_data.append(stock_entry)
    
    # Rank by EBIT/PPE (only stocks with EBIT/PPE data)
    ebit_ppe_stocks = [s for s in all_stock_data if s["ebit_ppe"] is not None]
    ebit_ppe_stocks.sort(key=lambda x: x["ebit_ppe"], reverse=True)
    total_ebit_ppe = len(ebit_ppe_stocks)
    for rank, stock in enumerate(ebit_ppe_stocks, start=1):
        stock["ebit_ppe_rank"] = rank
        stock["ebit_ppe_percentile"] = calculate_percentile(rank, total_ebit_ppe)
    
    # Rank by Operating Margin (only stocks with Operating Margin data)
    operating_margin_stocks = [s for s in all_stock_data if s["operating_margin"] is not None]
    operating_margin_stocks.sort(key=lambda x: x["operating_margin"], reverse=True)
    total_operating_margin = len(operating_margin_stocks)
    for rank, stock in enumerate(operating_margin_stocks, start=1):
        stock["operating_margin_rank"] = rank
        stock["operating_margin_percentile"] = calculate_percentile(rank, total_operating_margin)
    
    # Calculate combined ranks for stocks that have both metrics
    # For stocks with only one metric, use that metric's rank
    stocks_with_both = [s for s in all_stock_data if s["ebit_ppe"] is not None and s["operating_margin"] is not None]
    
    # Calculate average rank for stocks with both metrics
    for stock in stocks_with_both:
        avg_rank = (stock["ebit_ppe_rank"] + stock["operating_margin_rank"]) / 2.0
        stock["_combined_rank"] = avg_rank
    
    # Sort by combined rank (lower is better)
    stocks_with_both.sort(key=lambda x: x["_combined_rank"])
    
    # Assign total ranks and percentiles
    total_stocks_combined = len(stocks_with_both)
    for rank, stock in enumerate(stocks_with_both, start=1):
        stock["total_rank"] = rank
        stock["total_percentile"] = calculate_percentile(rank, total_stocks_combined)
    
    # For stocks with only one metric, set total_rank and total_percentile to None
    for stock in all_stock_data:
        if stock not in stocks_with_both:
            stock["total_rank"] = None
            stock["total_percentile"] = None
            # Also set missing individual ranks to None
            if stock["ebit_ppe"] is None:
                stock["ebit_ppe_rank"] = None
                stock["ebit_ppe_percentile"] = None
            if stock["operating_margin"] is None:
                stock["operating_margin_rank"] = None
                stock["operating_margin_percentile"] = None
    
    # Clean up temporary field
    for stock in all_stock_data:
        stock.pop("_combined_rank", None)
    
    return all_stock_data

def save_scores_to_json(scores_data: List[Dict], filename: str = "scores.json"):
    """
    Save calculated scores to JSON file
    
    Args:
        scores_data: List of dictionaries containing scores for each stock
        filename: Output filename
    """
    try:
        # Count stocks with each metric
        stocks_with_ebit_ppe = sum(1 for s in scores_data if s.get("ebit_ppe") is not None)
        stocks_with_operating_margin = sum(1 for s in scores_data if s.get("operating_margin") is not None)
        stocks_with_both = sum(1 for s in scores_data if s.get("ebit_ppe") is not None and s.get("operating_margin") is not None)
        
        output_data = {
            "metadata": {
                "total_stocks": len(scores_data),
                "stocks_with_ebit_ppe": stocks_with_ebit_ppe,
                "stocks_with_operating_margin": stocks_with_operating_margin,
                "stocks_with_both_metrics": stocks_with_both,
                "calculation_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "metrics": {
                    "ebit_ppe": "EBIT/PPE = Operating Income / PPE (most recent quarter)",
                    "operating_margin": "Operating Margin = Operating Income / Revenue (most recent quarter)",
                    "total_percentile": "Combined percentile based on average rank of EBIT/PPE and Operating Margin"
                },
                "description": "Percentiles where 100 is highest value, 0 is lowest. Total percentile combines ranks from both metrics."
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
    
    # EBIT/PPE
    if stock.get('ebit_ppe') is not None:
        print(f"\nEBIT/PPE: {stock['ebit_ppe']:.4f}")
        if stock.get('ebit_ppe_rank') is not None:
            print(f"  Rank: {stock['ebit_ppe_rank']:,}")
            print(f"  Percentile: {stock.get('ebit_ppe_percentile', 0):.2f}")
    else:
        print(f"\nEBIT/PPE: N/A")
    
    # Operating Margin
    if stock.get('operating_margin') is not None:
        print(f"\nOperating Margin: {stock['operating_margin']:.4f}")
        if stock.get('operating_margin_rank') is not None:
            print(f"  Rank: {stock['operating_margin_rank']:,}")
            print(f"  Percentile: {stock.get('operating_margin_percentile', 0):.2f}")
    else:
        print(f"\nOperating Margin: N/A")
    
    # Total Percentile
    if stock.get('total_percentile') is not None:
        print(f"\nTotal Percentile: {stock['total_percentile']:.2f}")
        if stock.get('total_rank') is not None:
            print(f"  Total Rank: {stock['total_rank']:,}")
    else:
        print(f"\nTotal Percentile: N/A (requires both EBIT/PPE and Operating Margin)")
    
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
    print(f"Total stocks: {len(scores_data)}")
    
    if scores_data:
        # EBIT/PPE Statistics
        ebit_ppe_values = [s["ebit_ppe"] for s in scores_data if s.get("ebit_ppe") is not None]
        if ebit_ppe_values:
            print(f"\nEBIT/PPE Statistics ({len(ebit_ppe_values)} stocks):")
            print(f"  Highest EBIT/PPE: {max(ebit_ppe_values):.4f}")
            print(f"  Lowest EBIT/PPE: {min(ebit_ppe_values):.4f}")
            print(f"  Median EBIT/PPE: {sorted(ebit_ppe_values)[len(ebit_ppe_values)//2]:.4f}")
            print(f"  Mean EBIT/PPE: {sum(ebit_ppe_values)/len(ebit_ppe_values):.4f}")
        
        # Operating Margin Statistics
        operating_margin_values = [s["operating_margin"] for s in scores_data if s.get("operating_margin") is not None]
        if operating_margin_values:
            print(f"\nOperating Margin Statistics ({len(operating_margin_values)} stocks):")
            print(f"  Highest Operating Margin: {max(operating_margin_values):.4f}")
            print(f"  Lowest Operating Margin: {min(operating_margin_values):.4f}")
            print(f"  Median Operating Margin: {sorted(operating_margin_values)[len(operating_margin_values)//2]:.4f}")
            print(f"  Mean Operating Margin: {sum(operating_margin_values)/len(operating_margin_values):.4f}")
        
        # Count by exchange
        nyse_count = sum(1 for s in scores_data if s.get("exchange") == "NYSE")
        nasdaq_count = sum(1 for s in scores_data if s.get("exchange") == "NASDAQ")
        print(f"\nBy Exchange:")
        print(f"  NYSE: {nyse_count} stocks")
        print(f"  NASDAQ: {nasdaq_count} stocks")
        
        # Show top 10 by Total Percentile (stocks with both metrics)
        stocks_with_total = [s for s in scores_data if s.get("total_percentile") is not None]
        stocks_with_total.sort(key=lambda x: x.get("total_percentile", 0), reverse=True)
        
        if stocks_with_total:
            print(f"\nTop 10 Stocks by Total Percentile (combines EBIT/PPE and Operating Margin):")
            for i, stock in enumerate(stocks_with_total[:10], 1):
                ebit_ppe_str = f"{stock['ebit_ppe']:.4f}" if stock.get('ebit_ppe') else "N/A"
                op_margin_str = f"{stock['operating_margin']:.4f}" if stock.get('operating_margin') else "N/A"
                print(f"  {i}. {stock['symbol']} ({stock['company_name']}): Total Percentile {stock['total_percentile']:.2f} (EBIT/PPE: {ebit_ppe_str}, Op Margin: {op_margin_str})")
    
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
        display_stock_info(stock)
    else:
        print(f"\nStock '{symbol}' not found in scores.json")
        print("Make sure you've run 'calc' first, and that the stock symbol is correct.\n")

def run_view_command(limit: Optional[int] = None):
    """
    Display all stocks ranked by total percentile (or individual percentiles if total not available)
    
    Args:
        limit: Optional limit on number of stocks to display (None = all)
    """
    scores_data = load_scores_from_json()
    if not scores_data:
        print(f"Error: scores.json not found. Please run 'calc' command first.\n")
        return
    
    scores = scores_data.get("scores", [])
    if not scores:
        print("No stock scores found in scores.json\n")
        return
    
    # Sort stocks: prioritize total_percentile, then ebit_ppe_percentile, then operating_margin_percentile
    def get_sort_key(stock):
        # Use total_percentile if available, otherwise use the best available percentile
        if stock.get("total_percentile") is not None:
            return (0, -stock["total_percentile"])  # Negative for descending sort
        elif stock.get("ebit_ppe_percentile") is not None:
            return (1, -stock["ebit_ppe_percentile"])
        elif stock.get("operating_margin_percentile") is not None:
            return (2, -stock["operating_margin_percentile"])
        else:
            return (3, 0)
    
    sorted_scores = sorted(scores, key=get_sort_key)
    
    # Apply limit if specified
    if limit:
        sorted_scores = sorted_scores[:limit]
    
    print(f"\n{'='*120}")
    print(f"All Stocks Ranked by Percentile" + (f" (showing top {limit})" if limit else ""))
    print(f"{'='*120}")
    print(f"{'Rank':<6} {'Symbol':<8} {'Company Name':<40} {'Total %':<10} {'EBIT/PPE %':<12} {'Op Margin %':<12} {'Exchange':<8}")
    print(f"{'-'*120}")
    
    for idx, stock in enumerate(sorted_scores, start=1):
        symbol = stock.get("symbol", "N/A")
        company_name = stock.get("company_name", "N/A")
        # Truncate company name if too long
        if len(company_name) > 38:
            company_name = company_name[:35] + "..."
        
        total_pct = f"{stock.get('total_percentile', 0):.2f}" if stock.get("total_percentile") is not None else "N/A"
        ebit_ppe_pct = f"{stock.get('ebit_ppe_percentile', 0):.2f}" if stock.get("ebit_ppe_percentile") is not None else "N/A"
        op_margin_pct = f"{stock.get('operating_margin_percentile', 0):.2f}" if stock.get("operating_margin_percentile") is not None else "N/A"
        exchange = stock.get("exchange", "N/A")
        
        print(f"{idx:<6} {symbol:<8} {company_name:<40} {total_pct:<10} {ebit_ppe_pct:<12} {op_margin_pct:<12} {exchange:<8}")
    
    print(f"{'='*120}")
    print(f"\nTotal stocks displayed: {len(sorted_scores)}")
    if limit and len(scores) > limit:
        print(f"Total stocks in database: {len(scores)}")
        print(f"Use 'view' without a number to see all stocks, or 'view <number>' to see top N stocks.\n")
    else:
        print()

def print_help():
    """Print help message with available commands"""
    print("\n" + "=" * 80)
    print("Available Commands:")
    print("=" * 80)
    print("  calc                   - Calculate and save scores for all stocks")
    print("  view [N]               - View all stocks ranked by percentile (optionally show top N)")
    print("  <symbol>               - Look up percentile rank for a stock (e.g., AAPL, MSFT)")
    print("  help                   - Show this help message")
    print("  exit / quit            - Exit the program")
    print("=" * 80 + "\n")

def main():
    """
    Main function with interactive command terminal
    
    Commands:
        calc        - Calculate and save scores for all stocks
        view [N]    - View all stocks ranked by percentile (optionally show top N)
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
            
            command_parts = user_input.split()
            command = command_parts[0].lower()
            
            # Handle commands
            if command == "exit" or command == "quit":
                print("\nExiting program. Goodbye!\n")
                break
            elif command == "help":
                print_help()
            elif command == "calc":
                run_calculate_command()
                print()  # Add blank line after command
            elif command == "view":
                # Check if user specified a limit
                limit = None
                if len(command_parts) > 1:
                    try:
                        limit = int(command_parts[1])
                        if limit <= 0:
                            print("Limit must be a positive number. Showing all stocks.\n")
                            limit = None
                    except ValueError:
                        print(f"Invalid limit '{command_parts[1]}'. Showing all stocks.\n")
                run_view_command(limit)
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

