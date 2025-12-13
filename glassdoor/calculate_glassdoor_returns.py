"""
Calculate buy-and-hold returns for Glassdoor Best Places to Work portfolios.

For each year, buys all stocks at the start of that year and holds forever.
When a stock delists/disappears, rebalances its value proportionally to remaining stocks.
"""
import json
import os
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

# Get paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
GLASSDOOR_DIR = SCRIPT_DIR
TICKERS_QUICKFS_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'tickers_quickfs')
RETURNS_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'returns')
RETURNS_CHARTS_DIR = os.path.join(RETURNS_DIR, 'charts')
RETURNS_JSONS_DIR = os.path.join(RETURNS_DIR, 'jsons')


def parse_date_string(date_str: str) -> Optional[int]:
    """Extract year from date string. Handles various formats."""
    if not date_str or date_str == '-':
        return None
    
    # Try YYYY-MM-DD format
    match = re.match(r'(\d{4})-\d{2}-\d{2}', str(date_str))
    if match:
        return int(match.group(1))
    
    # Try FY2009.FQ1 format
    match = re.match(r'FY(\d{4})\.FQ\d', str(date_str))
    if match:
        return int(match.group(1))
    
    # Try YYYY format
    match = re.match(r'(\d{4})', str(date_str))
    if match:
        return int(match.group(1))
    
    return None


def parse_date_to_datetime(date_str: str) -> Optional[datetime]:
    """Parse date string to datetime object."""
    if not date_str or date_str == '-':
        return None
    
    # Try YYYY-MM-DD format
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    
    # Try YYYY-MM format (quarterly data like "2003-03", "2003-06", etc.)
    match = re.match(r'^(\d{4})-(\d{2})$', str(date_str))
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return datetime(year, month, 1)
    
    # Try FY2009.FQ1 format - convert to first day of quarter
    match = re.match(r'FY(\d{4})\.FQ(\d)', str(date_str))
    if match:
        year = int(match.group(1))
        quarter = int(match.group(2))
        month = (quarter - 1) * 3 + 1
        return datetime(year, month, 1)
    
    # Try YYYY format (only as last resort)
    match = re.match(r'^(\d{4})$', str(date_str))
    if match:
        return datetime(int(match.group(1)), 1, 1)
    
    return None


def get_period_dates(data: Dict) -> Optional[List]:
    """Get period dates from stock data."""
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            return data[date_key]
    return None


def get_price_at_date(stock_data: Dict, target_date: datetime) -> Optional[Tuple[float, datetime, str]]:
    """
    Get price for a stock at or near a target date.
    
    Returns:
        Tuple of (price, actual_date, date_string) or None
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list):
        return None
    
    prices = data.get("period_end_price", [])
    if not isinstance(prices, list):
        return None
    
    # Find the closest date to target_date
    best_price = None
    best_date = None
    best_date_str = None
    min_diff = None
    
    for idx, date_str in enumerate(period_dates):
        if idx >= len(prices):
            continue
        
        price = prices[idx]
        if price is None or price <= 0:
            continue
        
        date_obj = parse_date_to_datetime(date_str)
        if date_obj is None:
            continue
        
        # Only consider dates on or after target_date
        if date_obj < target_date:
            continue
        
        diff = (date_obj - target_date).days
        if min_diff is None or diff < min_diff:
            min_diff = diff
            best_price = price
            best_date = date_obj
            best_date_str = date_str
    
    if best_price is not None:
        return (best_price, best_date, best_date_str)
    
    return None


def get_all_prices_over_time(stock_data: Dict, start_date: datetime) -> List[Tuple[datetime, float]]:
    """
    Get all prices for a stock from start_date onwards.
    
    Returns:
        List of (date, price) tuples, sorted by date
    """
    if not stock_data or "data" not in stock_data:
        return []
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list):
        return []
    
    prices = data.get("period_end_price", [])
    if not isinstance(prices, list):
        return []
    
    result = []
    for idx, date_str in enumerate(period_dates):
        if idx >= len(prices):
            continue
        
        price = prices[idx]
        if price is None or price <= 0:
            continue
        
        date_obj = parse_date_to_datetime(date_str)
        if date_obj is None or date_obj < start_date:
            continue
        
        result.append((date_obj, price))
    
    result.sort(key=lambda x: x[0])
    return result


def load_stock_data_by_ticker() -> Dict[str, Dict]:
    """Load all stock data from NYSE and NASDAQ files, indexed by ticker."""
    stock_dict = {}
    
    for data_file in ['nyse_data.jsonl', 'nasdaq_data.jsonl']:
        file_path = os.path.join(DATA_DIR, data_file)
        if not os.path.exists(file_path):
            continue
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            stock = json.loads(line)
                            ticker = stock.get('symbol', '').upper()
                            if ticker and ticker not in stock_dict:
                                stock_dict[ticker] = stock
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"Warning: Could not load {data_file}: {e}")
    
    return stock_dict


def calculate_portfolio_returns(year: int, stock_dict: Dict[str, Dict]) -> Dict:
    """
    Calculate buy-and-hold returns for a given year's Glassdoor portfolio.
    
    Args:
        year: Year to start the portfolio
        stock_dict: Dictionary of all stock data (ticker -> stock_data)
        
    Returns:
        Dictionary with portfolio value over time and statistics
    """
    # Load ticker data for the year
    ticker_file = os.path.join(TICKERS_QUICKFS_DIR, f'glassdoor_{year}_tickers.json')
    if not os.path.exists(ticker_file):
        print(f"Error: Ticker file not found: {ticker_file}")
        return None
    
    with open(ticker_file, 'r', encoding='utf-8') as f:
        ticker_data = json.load(f)
    
    matched = ticker_data.get('matched', [])
    if not matched:
        print(f"No matched companies for year {year}")
        return None
    
    # Get all tickers
    tickers = [m['ticker'] for m in matched]
    print(f"\nCalculating returns for {year}: {len(tickers)} stocks")
    
    # Start date: January 1st of the year
    start_date = datetime(year, 1, 1)
    
    # Get initial prices for all stocks
    initial_prices = {}
    initial_shares = {}
    initial_value = {}
    total_initial_value = 0.0
    
    for ticker in tickers:
        if ticker not in stock_dict:
            print(f"  Warning: {ticker} not found in stock data")
            continue
        
        stock = stock_dict[ticker]
        price_data = get_price_at_date(stock, start_date)
        
        if price_data is None:
            # Try to get earliest available price after start_date
            all_prices = get_all_prices_over_time(stock, start_date)
            if all_prices:
                price_data = (all_prices[0][1], all_prices[0][0], str(all_prices[0][0]))
            else:
                print(f"  Warning: No price data for {ticker}")
                continue
        
        price, date_obj, date_str = price_data
        initial_prices[ticker] = price
        # Buy $1000 worth of each stock initially (equal weight)
        shares = 1000.0 / price
        initial_shares[ticker] = shares
        initial_value[ticker] = 1000.0
        total_initial_value += 1000.0
    
    if total_initial_value == 0:
        print(f"  Error: No valid initial prices found")
        return None
    
    print(f"  Initial portfolio value: ${total_initial_value:,.2f}")
    print(f"  Initial stocks: {len(initial_prices)}")
    
    # Collect all price points over time
    price_history = {}  # ticker -> list of (date, price) tuples
    all_available_dates = set()
    
    for ticker in initial_prices.keys():
        prices = get_all_prices_over_time(stock_dict[ticker], start_date)
        price_history[ticker] = prices
        for date, _ in prices:
            all_available_dates.add(date)
    
    # Debug: Check data granularity
    total_price_points = sum(len(prices) for prices in price_history.values())
    print(f"  Total quarterly price points collected: {total_price_points}")
    
    if not all_available_dates:
        print(f"  Error: No price data found")
        return None
    
    # Use the first actual data date as the effective start, not Jan 1
    # This ensures we start from when we actually have price data
    first_data_date = min(all_available_dates)
    end_date = max(all_available_dates)
    
    print(f"  Data range: {first_data_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Generate quarterly dates starting from the first actual data date
    quarterly_dates = []
    current_quarter = first_data_date
    
    # Generate quarterly dates (Q1, Q2, Q3, Q4 for each year)
    while current_quarter <= end_date:
        quarterly_dates.append(current_quarter)
        # Move to next quarter (3 months later)
        if current_quarter.month <= 3:
            next_month = 4
            next_year = current_quarter.year
        elif current_quarter.month <= 6:
            next_month = 7
            next_year = current_quarter.year
        elif current_quarter.month <= 9:
            next_month = 10
            next_year = current_quarter.year
        else:
            next_month = 1
            next_year = current_quarter.year + 1
        
        current_quarter = datetime(next_year, next_month, 1)
    
    # Also include all actual data dates to capture any non-standard quarters
    all_dates = sorted(set(quarterly_dates + list(all_available_dates)))
    
    # Calculate portfolio value over time
    portfolio_values = []
    active_tickers = set(initial_prices.keys())
    current_shares = initial_shares.copy()
    
    # Add initial portfolio value at the first data date (not Jan 1)
    portfolio_values.append((first_data_date, total_initial_value))
    
    # Track last known prices for each stock (start with initial prices)
    last_known_prices = initial_prices.copy()
    
    for date in all_dates:
        # Skip if this is the first_data_date (we already added initial value)
        if date == first_data_date:
            continue
            
        # Get current prices for all active stocks
        active_prices = {}  # ticker -> price at this date
        
        for ticker in active_tickers:
            if ticker not in price_history:
                continue
            
            # Find price at or before this date
            current_price = None
            for price_date, price in price_history[ticker]:
                if price_date <= date:
                    current_price = price
                else:
                    break
            
            if current_price is not None:
                # Found actual price data
                active_prices[ticker] = current_price
                last_known_prices[ticker] = current_price
            elif ticker in last_known_prices:
                # No price yet for this date, use last known price (could be initial price)
                # This handles stocks that start later in the year
                active_prices[ticker] = last_known_prices[ticker]
        
        # Check for truly disappeared stocks (had data before but now stopped trading)
        # A stock is truly gone if it stopped trading well before the data collection ended
        # (not just because it's missing the most recent quarter)
        still_active = set()
        disappeared_tickers = set()
        
        # Find the most recent data date across all stocks (data collection end date)
        all_last_dates = [price_history[t][-1][0] for t in price_history if price_history[t]]
        latest_data_date = max(all_last_dates) if all_last_dates else date
        
        for ticker in active_tickers:
            if ticker in price_history and len(price_history[ticker]) > 0:
                last_price_date = price_history[ticker][-1][0]
                
                # Only mark as disappeared if data stopped >6 months before the latest data
                # This accounts for data that just hasn't been updated yet vs truly delisted stocks
                months_behind = (latest_data_date.year - last_price_date.year) * 12 + \
                               (latest_data_date.month - last_price_date.month)
                
                if last_price_date < date and months_behind > 6:
                    # Stock has ended - mark as disappeared
                    disappeared_tickers.add(ticker)
                else:
                    # Stock still active (or just missing recent data)
                    still_active.add(ticker)
            elif ticker in active_prices:
                # Stock still active (using initial price)
                still_active.add(ticker)
        
        # If some stocks truly disappeared (stopped trading), rebalance
        if disappeared_tickers:
            disappeared_value = 0.0
            for ticker in disappeared_tickers:
                if ticker in current_shares and ticker in last_known_prices:
                    disappeared_value += current_shares[ticker] * last_known_prices[ticker]
                    del current_shares[ticker]
                    if ticker in active_prices:
                        del active_prices[ticker]
            
            # Rebalance disappeared value proportionally to remaining stocks
            if disappeared_value > 0 and len(still_active) > 0:
                total_active_value = sum(
                    current_shares[t] * active_prices.get(t, last_known_prices.get(t, 0))
                    for t in still_active
                    if t in current_shares
                )
                
                if total_active_value > 0:
                    for ticker in still_active:
                        if ticker in current_shares:
                            price = active_prices.get(ticker, last_known_prices.get(ticker, 0))
                            if price > 0:
                                current_value = current_shares[ticker] * price
                                proportion = current_value / total_active_value
                                additional_value = disappeared_value * proportion
                                additional_shares = additional_value / price
                                current_shares[ticker] += additional_shares
        
        # Update active tickers
        active_tickers = still_active
        
        # Calculate current portfolio value
        portfolio_value = sum(
            current_shares.get(t, 0) * active_prices.get(t, last_known_prices.get(t, 0))
            for t in active_tickers
        )
        
        # Add portfolio value
        if portfolio_value > 0:
            portfolio_values.append((date, portfolio_value))
    
    if not portfolio_values:
        print(f"  Error: No portfolio values calculated")
        return None
    
    print(f"  Portfolio values calculated for {len(portfolio_values)} time periods")
    
    # Calculate returns
    final_value = portfolio_values[-1][1]
    total_return = (final_value / total_initial_value - 1) * 100
    annualized_return = None
    
    if len(portfolio_values) > 1:
        years = (portfolio_values[-1][0] - portfolio_values[0][0]).days / 365.25
        if years > 0:
            annualized_return = ((final_value / total_initial_value) ** (1 / years) - 1) * 100
    
    result = {
        'year': year,
        'initial_value': total_initial_value,
        'final_value': final_value,
        'total_return_pct': total_return,
        'annualized_return_pct': annualized_return,
        'num_stocks': len(initial_prices),
        'final_num_stocks': len(active_tickers),
        'portfolio_values': [(d.isoformat(), v) for d, v in portfolio_values]
    }
    
    print(f"  Final portfolio value: ${final_value:,.2f}")
    print(f"  Total return: {total_return:.2f}%")
    if annualized_return:
        print(f"  Annualized return: {annualized_return:.2f}%")
    print(f"  Final stocks: {len(active_tickers)}")
    
    return result


def create_returns_chart(results: Dict, output_dir: str):
    """Create a chart showing portfolio returns over time."""
    if not results or 'portfolio_values' not in results:
        return
    
    year = results['year']
    portfolio_values = results['portfolio_values']
    
    if not portfolio_values:
        return
    
    # Parse dates and values
    dates = [datetime.fromisoformat(d) for d, _ in portfolio_values]
    values = [v for _, v in portfolio_values]
    
    # Calculate returns as percentage
    initial_value = results['initial_value']
    returns_pct = [(v / initial_value - 1) * 100 for v in values]
    
    # Create chart
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Plot with quarterly markers - show markers more frequently for better granularity
    marker_frequency = max(1, len(dates)//30)  # Show more markers
    ax.plot(dates, returns_pct, linewidth=2, color='#2E86AB', marker='o', markersize=2.5, markevery=marker_frequency, alpha=0.9)
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax.fill_between(dates, 0, returns_pct, alpha=0.3, color='#2E86AB')
    
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Total Return (%)', fontsize=12)
    ax.set_title(f'Glassdoor Best Places to Work {year} - Buy & Hold Returns (Quarterly Data)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    ax.grid(True, alpha=0.1, which='minor')
    
    # Format x-axis dates with quarterly granularity
    # Always show quarterly information in labels
    date_range = (dates[-1] - dates[0]).days
    years_span = date_range / 365.25
    
    # Custom formatter to always show quarters
    def format_quarterly(x, pos=None):
        dt = mdates.num2date(x)
        quarter = (dt.month - 1) // 3 + 1
        # Always show quarter information
        return f'{dt.year} Q{quarter}'
    
    # Set quarterly minor ticks always for visual granularity
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=3))
    
    # Determine major tick interval based on range
    if years_span <= 3:
        # Short range: show every quarter
        major_interval = 3
    elif years_span <= 8:
        # Medium range: show every 2 quarters (6 months)
        major_interval = 6
    else:
        # Long range: show every 4 quarters (yearly) but label with quarters
        major_interval = 12
    
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=major_interval))
    ax.xaxis.set_major_formatter(FuncFormatter(format_quarterly))
    plt.xticks(rotation=45, ha='right')
    
    # Make markers more visible for quarterly data points
    # Update the existing plot line to show quarterly markers more clearly
    for line in ax.lines:
        if line.get_label() == '' or 'returns' in line.get_label().lower():
            line.set_marker('o')
            line.set_markersize(2.5)
            line.set_markevery(max(1, len(dates)//40))
            line.set_alpha(0.9)
    
    # Add statistics text
    stats_text = f"Initial: ${results['initial_value']:,.0f} | "
    stats_text += f"Final: ${results['final_value']:,.0f} | "
    stats_text += f"Return: {results['total_return_pct']:.1f}%"
    if results.get('annualized_return_pct'):
        stats_text += f" | Annualized: {results['annualized_return_pct']:.1f}%"
    stats_text += f"\nStocks: {results['num_stocks']} → {results['final_num_stocks']}"
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
            fontsize=9, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    
    # Save chart
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'glassdoor_{year}_returns.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Chart saved: {output_file}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate buy-and-hold returns for Glassdoor portfolios')
    parser.add_argument('--year', type=int, help='Year to calculate (2009-2025)')
    parser.add_argument('--all', action='store_true', help='Process all years')
    
    args = parser.parse_args()
    
    current_year = datetime.now().year
    
    # Load stock data once
    print("Loading stock data from NYSE and NASDAQ files...")
    stock_dict = load_stock_data_by_ticker()
    print(f"Loaded {len(stock_dict)} stocks")
    
    if args.all:
        # Find all available years
        years = []
        for filename in os.listdir(TICKERS_QUICKFS_DIR):
            if filename.startswith('glassdoor_') and filename.endswith('_tickers.json'):
                try:
                    year = int(filename.split('_')[1])
                    if 2009 <= year <= current_year:
                        years.append(year)
                except (ValueError, IndexError):
                    continue
        years = sorted(set(years))
    elif args.year:
        if 2009 <= args.year <= current_year:
            years = [args.year]
        else:
            print(f"Error: Year must be between 2009 and {current_year}")
            return
    else:
        # Interactive mode
        print("Glassdoor Portfolio Returns Calculator")
        print("=" * 60)
        print(f"Enter a year (2009-{current_year}) or 'all' for all years")
        print("=" * 60)
        
        while True:
            try:
                year_input = input(f"\nEnter the year (2009-{current_year}) or 'all' (or 'quit' to stop): ").strip().lower()
                
                if year_input in ['quit', 'exit', 'q']:
                    print("\nExiting. Goodbye!")
                    return
                
                if year_input == 'all':
                    years = []
                    for filename in os.listdir(TICKERS_QUICKFS_DIR):
                        if filename.startswith('glassdoor_') and filename.endswith('_tickers.json'):
                            try:
                                year = int(filename.split('_')[1])
                                if 2009 <= year <= current_year:
                                    years.append(year)
                            except (ValueError, IndexError):
                                continue
                    years = sorted(set(years))
                    break
                else:
                    year = int(year_input)
                    if 2009 <= year <= current_year:
                        years = [year]
                        break
                    else:
                        print(f"Error: Year must be between 2009 and {current_year}. Please try again.")
            except ValueError:
                print(f"Error: '{year_input}' is not a valid year. Please enter a number between 2009 and {current_year}, 'all', or 'quit'.")
            except KeyboardInterrupt:
                print("\n\nCalculator cancelled by user. Exiting...")
                return
    
    # Process each year
    all_results = []
    for year in years:
        try:
            result = calculate_portfolio_returns(year, stock_dict)
            if result:
                all_results.append(result)
                create_returns_chart(result, RETURNS_CHARTS_DIR)
                
                # Save results to JSON
                os.makedirs(RETURNS_JSONS_DIR, exist_ok=True)
                results_file = os.path.join(RETURNS_JSONS_DIR, f'glassdoor_{year}_returns.json')
                with open(results_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                print(f"  Results saved: {results_file}")
        except Exception as e:
            print(f"\nError processing {year}: {e}")
            import traceback
            traceback.print_exc()
    
    if len(years) > 1:
        print(f"\n{'='*60}")
        print(f"Completed processing {len(all_results)} years")
        print(f"{'='*60}")


if __name__ == '__main__':
    main()

