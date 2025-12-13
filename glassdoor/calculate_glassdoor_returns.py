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
import numpy as np

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
    
    # Calculate individual stock returns
    individual_stock_returns = []
    for ticker in initial_prices.keys():
        if ticker not in price_history or not price_history[ticker]:
            continue
        
        # Get company name from matched data
        company_name = ticker  # default to ticker
        for m in matched:
            if m['ticker'] == ticker:
                company_name = m.get('glassdoor_name', ticker)
                break
        
        initial_price = initial_prices[ticker]
        prices = price_history[ticker]
        
        # Get first and last price dates/values
        first_price_date, first_price = prices[0]
        last_price_date, last_price = prices[-1]
        
        # Calculate total return
        stock_total_return = (last_price / initial_price - 1) * 100
        
        # Calculate annualized return
        stock_years = (last_price_date - first_price_date).days / 365.25
        if stock_years > 0:
            stock_annualized = ((last_price / initial_price) ** (1 / stock_years) - 1) * 100
        else:
            stock_annualized = 0
        
        individual_stock_returns.append({
            'ticker': ticker,
            'company': company_name,
            'initial_price': initial_price,
            'final_price': last_price,
            'first_date': first_price_date.isoformat(),
            'last_date': last_price_date.isoformat(),
            'years_held': round(stock_years, 2),
            'total_return_pct': round(stock_total_return, 2),
            'annualized_return_pct': round(stock_annualized, 2)
        })
    
    # Sort by annualized return descending
    individual_stock_returns.sort(key=lambda x: x['annualized_return_pct'], reverse=True)
    
    result = {
        'year': year,
        'initial_value': total_initial_value,
        'final_value': final_value,
        'total_return_pct': total_return,
        'annualized_return_pct': annualized_return,
        'num_stocks': len(initial_prices),
        'final_num_stocks': len(active_tickers),
        'portfolio_values': [(d.isoformat(), v) for d, v in portfolio_values],
        'individual_stock_returns': individual_stock_returns
    }
    
    print(f"  Final portfolio value: ${final_value:,.2f}")
    print(f"  Total return: {total_return:.2f}%")
    if annualized_return:
        print(f"  Annualized return: {annualized_return:.2f}%")
    print(f"  Final stocks: {len(active_tickers)}")
    
    return result


def load_benchmark_data() -> Optional[Dict]:
    """Load benchmark data from JSON file."""
    benchmark_file = os.path.join(GLASSDOOR_DIR, 'data', 'benchmark', 'benchmark_top500_returns.json')
    if not os.path.exists(benchmark_file):
        return None
    
    try:
        with open(benchmark_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load benchmark data: {e}")
        return None


def get_benchmark_returns_for_period(benchmark_data: Dict, start_date: datetime, end_date: datetime) -> List[Tuple[datetime, float]]:
    """
    Get benchmark returns normalized to start at 0% from the given start date.
    
    Returns:
        List of (date, return_pct) tuples
    """
    if not benchmark_data or 'portfolio_values' not in benchmark_data:
        return []
    
    # Parse benchmark values
    benchmark_values = []
    for date_str, value in benchmark_data['portfolio_values']:
        try:
            date = datetime.fromisoformat(date_str)
            benchmark_values.append((date, value))
        except:
            continue
    
    benchmark_values.sort(key=lambda x: x[0])
    
    # Filter to the period we need
    filtered = [(d, v) for d, v in benchmark_values if start_date <= d <= end_date]
    
    if not filtered:
        return []
    
    # Find the benchmark value at or just before start_date to normalize
    start_value = None
    for d, v in benchmark_values:
        if d <= start_date:
            start_value = v
        else:
            break
    
    # If no value before start_date, use the first available value in the period
    if start_value is None and filtered:
        start_value = filtered[0][1]
    
    if start_value is None or start_value <= 0:
        return []
    
    # Calculate returns as percentage from start
    returns = [(d, (v / start_value - 1) * 100) for d, v in filtered]
    
    return returns


def create_returns_chart(results: Dict, output_dir: str, benchmark_data: Optional[Dict] = None):
    """Create a chart showing portfolio returns over time with benchmark comparison."""
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
    
    # Plot Glassdoor returns with quarterly markers
    marker_frequency = max(1, len(dates)//30)
    ax.plot(dates, returns_pct, linewidth=2.5, color='#2E86AB', marker='o', markersize=3, 
            markevery=marker_frequency, alpha=0.9, label=f'Glassdoor {year}')
    ax.fill_between(dates, 0, returns_pct, alpha=0.2, color='#2E86AB')
    
    # Add benchmark comparison if available
    benchmark_returns = []
    bench_returns = []
    if benchmark_data:
        benchmark_returns = get_benchmark_returns_for_period(benchmark_data, dates[0], dates[-1])
        if benchmark_returns:
            bench_dates = [d for d, _ in benchmark_returns]
            bench_returns = [r for _, r in benchmark_returns]
            ax.plot(bench_dates, bench_returns, linewidth=2, color='#e74c3c', 
                    linestyle='--', alpha=0.8, label='Benchmark')
    
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Total Return (%)', fontsize=12)
    ax.set_title(f'Glassdoor Best Places to Work {year} - Buy & Hold Returns vs Benchmark', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    ax.grid(True, alpha=0.1, which='minor')
    
    # Format x-axis dates with quarterly granularity
    date_range = (dates[-1] - dates[0]).days
    years_span = date_range / 365.25
    
    # Custom formatter to always show quarters
    def format_quarterly(x, pos=None):
        dt = mdates.num2date(x)
        quarter = (dt.month - 1) // 3 + 1
        return f'{dt.year} Q{quarter}'
    
    # Set quarterly minor ticks always for visual granularity
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=3))
    
    # Determine major tick interval based on range
    if years_span <= 3:
        major_interval = 3
    elif years_span <= 8:
        major_interval = 6
    else:
        major_interval = 12
    
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=major_interval))
    ax.xaxis.set_major_formatter(FuncFormatter(format_quarterly))
    plt.xticks(rotation=45, ha='right')
    
    # Build stats text box with all info (no separate legend)
    glassdoor_final = results['total_return_pct']
    glassdoor_ann = results.get('annualized_return_pct', 0)
    
    stats_lines = [
        f"Glassdoor {year}:",
        f"  Total: {glassdoor_final:,.0f}%  |  Ann: {glassdoor_ann:.1f}%",
        f"  Stocks: {results['num_stocks']} → {results['final_num_stocks']}"
    ]
    
    if benchmark_returns:
        bench_final = bench_returns[-1]
        # Calculate benchmark annualized
        bench_years = (dates[-1] - dates[0]).days / 365.25
        if bench_years > 0:
            bench_ann = ((1 + bench_final/100) ** (1/bench_years) - 1) * 100
        else:
            bench_ann = 0
        stats_lines.append("")
        stats_lines.append("Benchmark (Top 500):")
        stats_lines.append(f"  Total: {bench_final:,.0f}%  |  Ann: {bench_ann:.1f}%")
        
        # Add outperformance
        beat = glassdoor_ann - bench_ann
        stats_lines.append("")
        stats_lines.append(f"Outperformance: {beat:+.1f}%/yr")
    
    stats_text = "\n".join(stats_lines)
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
            fontsize=9, verticalalignment='top', family='monospace',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='gray'))
    
    # Simple legend for line identification
    ax.legend(loc='lower right', fontsize=9, framealpha=0.9)
    
    plt.tight_layout()
    
    # Save chart
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f'glassdoor_{year}_returns.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Chart saved: {output_file}")


def create_summary_chart(all_results: List[Dict], output_dir: str):
    """Create a summary chart comparing all years' performance."""
    if not all_results:
        return
    
    # Sort by year
    all_results = sorted(all_results, key=lambda x: x['year'])
    
    years = [r['year'] for r in all_results]
    year_labels = [str(y)[2:] for y in years]  # Use '09, '10, etc. for compactness
    total_returns = [r['total_return_pct'] for r in all_results]
    annualized_returns = [r.get('annualized_return_pct', 0) for r in all_results]
    initial_stocks = [r['num_stocks'] for r in all_results]
    final_stocks = [r['final_num_stocks'] for r in all_results]
    
    # Create figure with subplots - larger size for better spacing
    fig, axes = plt.subplots(2, 2, figsize=(18, 13))
    fig.suptitle('Glassdoor Best Places to Work - Portfolio Returns Summary', fontsize=18, fontweight='bold', y=0.98)
    plt.subplots_adjust(hspace=0.35, wspace=0.25)
    
    # Color palette
    colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(years)))
    
    # 1. Total Return Bar Chart
    ax1 = axes[0, 0]
    x1 = np.arange(len(years))
    bars1 = ax1.bar(x1, total_returns, color=colors, edgecolor='black', linewidth=0.5, width=0.7)
    ax1.set_xlabel('Year', fontsize=11)
    ax1.set_ylabel('Total Return (%)', fontsize=11)
    ax1.set_title('Total Return by Year', fontsize=13, fontweight='bold')
    ax1.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.set_xticks(x1)
    ax1.set_xticklabels([f"'{y}" for y in year_labels], fontsize=9)
    
    # Add value labels - inside bar for tall bars, above for short
    max_return = max(total_returns)
    for bar, val in zip(bars1, total_returns):
        height = bar.get_height()
        if height > max_return * 0.4:  # Tall bar - put label inside
            ax1.annotate(f'{val:.0f}%',
                        xy=(bar.get_x() + bar.get_width() / 2, height - max_return * 0.05),
                        ha='center', va='top', fontsize=7, fontweight='bold', color='white')
        else:  # Short bar - put label above
            ax1.annotate(f'{val:.0f}%',
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 2), textcoords="offset points",
                        ha='center', va='bottom', fontsize=7)
    
    # Add some padding at top for labels
    ax1.set_ylim(top=max_return * 1.08)
    
    # 2. Annualized Return Bar Chart
    ax2 = axes[0, 1]
    x2 = np.arange(len(years))
    bars2 = ax2.bar(x2, annualized_returns, color=colors, edgecolor='black', linewidth=0.5, width=0.7)
    ax2.set_xlabel('Year', fontsize=11)
    ax2.set_ylabel('Annualized Return (%)', fontsize=11)
    ax2.set_title('Annualized Return by Year', fontsize=13, fontweight='bold')
    ax2.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax2.axhline(y=10, color='#e74c3c', linestyle='--', linewidth=2, alpha=0.8, label='10% benchmark')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_xticks(x2)
    ax2.set_xticklabels([f"'{y}" for y in year_labels], fontsize=9)
    ax2.legend(loc='upper right', fontsize=9)
    
    # Add value labels above bars (horizontal, no rotation)
    max_ann = max(annualized_returns)
    for bar, val in zip(bars2, annualized_returns):
        height = bar.get_height()
        ax2.annotate(f'{val:.1f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 2), textcoords="offset points",
                    ha='center', va='bottom', fontsize=7)
    
    # Add padding at top
    ax2.set_ylim(top=max_ann * 1.15)
    
    # 3. Stock Retention Chart
    ax3 = axes[1, 0]
    x3 = np.arange(len(years))
    width = 0.35
    bars3a = ax3.bar(x3 - width/2, initial_stocks, width, label='Initial Stocks', color='#3498db', edgecolor='black', linewidth=0.5)
    bars3b = ax3.bar(x3 + width/2, final_stocks, width, label='Final Stocks', color='#2ecc71', edgecolor='black', linewidth=0.5)
    ax3.set_xlabel('Year', fontsize=11)
    ax3.set_ylabel('Number of Stocks', fontsize=11)
    ax3.set_title('Stock Retention by Year', fontsize=13, fontweight='bold')
    ax3.set_xticks(x3)
    ax3.set_xticklabels([f"'{y}" for y in year_labels], fontsize=9)
    ax3.legend(loc='upper left', fontsize=9)
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Add padding at top
    ax3.set_ylim(top=max(initial_stocks) * 1.1)
    
    # 4. Summary Statistics Table
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Calculate summary stats
    avg_total_return = np.mean(total_returns)
    avg_annualized = np.mean(annualized_returns)
    best_year_idx = np.argmax(total_returns)
    worst_year_idx = np.argmin(total_returns)
    best_ann_idx = np.argmax(annualized_returns)
    avg_retention = np.mean([f/i*100 for i, f in zip(initial_stocks, final_stocks)])
    
    # Create table data
    table_data = [
        ['Metric', 'Value'],
        ['Years Analyzed', f'{len(years)} ({min(years)}-{max(years)})'],
        ['Avg Total Return', f'{avg_total_return:,.0f}%'],
        ['Avg Annualized Return', f'{avg_annualized:.1f}%'],
        ['Best Year (Total)', f'{years[best_year_idx]} ({total_returns[best_year_idx]:,.0f}%)'],
        ['Best Year (Annualized)', f'{years[best_ann_idx]} ({annualized_returns[best_ann_idx]:.1f}%)'],
        ['Worst Year (Total)', f'{years[worst_year_idx]} ({total_returns[worst_year_idx]:.0f}%)'],
        ['Avg Stock Retention', f'{avg_retention:.1f}%'],
        ['Total Stocks Analyzed', f'{sum(initial_stocks):,}'],
    ]
    
    # Create table
    table = ax4.table(
        cellText=table_data,
        cellLoc='center',
        loc='center',
        colWidths=[0.55, 0.45]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1.3, 2.2)
    
    # Style header row
    for i in range(2):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(color='white', fontweight='bold', fontsize=13)
    
    # Alternate row colors and style
    for i in range(1, len(table_data)):
        color = '#e8f4f8' if i % 2 == 0 else 'white'
        for j in range(2):
            table[(i, j)].set_facecolor(color)
            table[(i, j)].set_text_props(fontsize=11)
    
    ax4.set_title('Summary Statistics', fontsize=13, fontweight='bold', pad=10)
    
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save chart
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'glassdoor_returns_summary.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nSummary chart saved: {output_file}")
    
    # Also save summary JSON
    summary_data = {
        'years_analyzed': len(years),
        'year_range': f'{min(years)}-{max(years)}',
        'avg_total_return_pct': avg_total_return,
        'avg_annualized_return_pct': avg_annualized,
        'best_year': {
            'year': years[best_year_idx],
            'total_return_pct': total_returns[best_year_idx],
            'annualized_return_pct': annualized_returns[best_year_idx]
        },
        'worst_year': {
            'year': years[worst_year_idx],
            'total_return_pct': total_returns[worst_year_idx],
            'annualized_return_pct': annualized_returns[worst_year_idx]
        },
        'avg_stock_retention_pct': avg_retention,
        'total_stocks_analyzed': sum(initial_stocks),
        'by_year': [
            {
                'year': r['year'],
                'initial_stocks': r['num_stocks'],
                'final_stocks': r['final_num_stocks'],
                'total_return_pct': r['total_return_pct'],
                'annualized_return_pct': r.get('annualized_return_pct', 0)
            }
            for r in all_results
        ]
    }
    
    summary_json_file = os.path.join(RETURNS_JSONS_DIR, 'glassdoor_returns_summary.json')
    with open(summary_json_file, 'w', encoding='utf-8') as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    print(f"Summary JSON saved: {summary_json_file}")


def create_benchmark_beat_chart(all_results: List[Dict], benchmark_data: Optional[Dict], output_dir: str):
    """Create a chart showing annualized outperformance vs benchmark for each year."""
    if not all_results or not benchmark_data:
        print("Cannot create benchmark beat chart: missing data")
        return
    
    # Sort by year
    all_results = sorted(all_results, key=lambda x: x['year'])
    
    # Calculate benchmark annualized return for each Glassdoor period
    beat_data = []
    
    for result in all_results:
        year = result['year']
        glassdoor_ann = result.get('annualized_return_pct', 0)
        
        # Get the period dates from the result
        portfolio_values = result.get('portfolio_values', [])
        if len(portfolio_values) < 2:
            continue
        
        start_date = datetime.fromisoformat(portfolio_values[0][0])
        end_date = datetime.fromisoformat(portfolio_values[-1][0])
        
        # Calculate benchmark return for the same period
        benchmark_returns = get_benchmark_returns_for_period(benchmark_data, start_date, end_date)
        
        if benchmark_returns:
            # Get total return from benchmark for this period
            bench_total_return_pct = benchmark_returns[-1][1]  # Already in percent
            
            # Calculate annualized from total return
            years_elapsed = (end_date - start_date).days / 365.25
            if years_elapsed > 0:
                # Convert percentage back to multiplier, then annualize
                bench_multiplier = 1 + (bench_total_return_pct / 100)
                bench_ann = (bench_multiplier ** (1 / years_elapsed) - 1) * 100
            else:
                bench_ann = 0
            
            beat = glassdoor_ann - bench_ann
            beat_data.append({
                'year': year,
                'glassdoor_ann': glassdoor_ann,
                'benchmark_ann': bench_ann,
                'beat': beat
            })
    
    if not beat_data:
        print("No benchmark beat data calculated")
        return
    
    # Create chart
    fig, ax = plt.subplots(figsize=(14, 8))
    
    years = [d['year'] for d in beat_data]
    year_labels = [str(y)[2:] for y in years]
    beats = [d['beat'] for d in beat_data]
    glassdoor_anns = [d['glassdoor_ann'] for d in beat_data]
    benchmark_anns = [d['benchmark_ann'] for d in beat_data]
    
    x = np.arange(len(years))
    
    # Color bars based on positive/negative beat
    colors = ['#2ecc71' if b >= 0 else '#e74c3c' for b in beats]
    
    bars = ax.bar(x, beats, color=colors, edgecolor='black', linewidth=0.5, width=0.7)
    
    # Add zero line
    ax.axhline(y=0, color='black', linestyle='-', linewidth=1.5)
    
    # Add value labels
    for bar, val, gd, bm in zip(bars, beats, glassdoor_anns, benchmark_anns):
        height = bar.get_height()
        # Position label above or below bar depending on sign
        if height >= 0:
            va = 'bottom'
            y_offset = 0.3
        else:
            va = 'top'
            y_offset = -0.3
        
        ax.annotate(f'{val:+.1f}%',
                    xy=(bar.get_x() + bar.get_width() / 2, height + y_offset),
                    ha='center', va=va, fontsize=9, fontweight='bold')
        
        # Add smaller annotation showing actual values
        ax.annotate(f'({gd:.1f} vs {bm:.1f})',
                    xy=(bar.get_x() + bar.get_width() / 2, 0),
                    xytext=(0, -18), textcoords="offset points",
                    ha='center', va='top', fontsize=7, color='gray')
    
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Annualized Outperformance (%)', fontsize=12)
    ax.set_title('Glassdoor Best Places to Work vs Benchmark\nAnnualized Return Difference by Year', 
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f"'{y}" for y in year_labels], fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add padding
    max_abs = max(abs(min(beats)), abs(max(beats)))
    ax.set_ylim(-max_abs * 1.3, max_abs * 1.3)
    
    # Add summary stats
    avg_beat = np.mean(beats)
    median_beat = np.median(beats)
    years_outperformed = sum(1 for b in beats if b > 0)
    
    stats_text = f"Average Beat: {avg_beat:+.1f}% per year\n"
    stats_text += f"Median Beat: {median_beat:+.1f}% per year\n"
    stats_text += f"Years Outperformed: {years_outperformed}/{len(beats)} ({years_outperformed/len(beats)*100:.0f}%)"
    
    ax.text(0.98, 0.98, stats_text, transform=ax.transAxes,
            fontsize=11, verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#2ecc71', edgecolor='black', label='Outperformed'),
        Patch(facecolor='#e74c3c', edgecolor='black', label='Underperformed')
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=10)
    
    plt.tight_layout()
    
    # Save chart
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'glassdoor_benchmark_beat.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nBenchmark beat chart saved: {output_file}")
    
    # Also save beat data to JSON
    beat_json = {
        'average_beat_pct': avg_beat,
        'median_beat_pct': median_beat,
        'years_outperformed': years_outperformed,
        'total_years': len(beats),
        'outperformance_rate_pct': years_outperformed / len(beats) * 100,
        'by_year': beat_data
    }
    
    beat_json_file = os.path.join(RETURNS_JSONS_DIR, 'glassdoor_benchmark_beat.json')
    with open(beat_json_file, 'w', encoding='utf-8') as f:
        json.dump(beat_json, f, indent=2, ensure_ascii=False)
    print(f"Benchmark beat JSON saved: {beat_json_file}")


def main():
    """Main function."""
    current_year = datetime.now().year
    
    # Load stock data once
    print("Loading stock data from NYSE and NASDAQ files...")
    stock_dict = load_stock_data_by_ticker()
    print(f"Loaded {len(stock_dict)} stocks")
    
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
    
    # Load benchmark data for comparison
    print("\nLoading benchmark data for comparison...")
    benchmark_data = load_benchmark_data()
    if benchmark_data:
        print(f"Benchmark data loaded: {benchmark_data.get('start_year', '?')}-{benchmark_data.get('end_year', '?')}")
    else:
        print("Warning: Benchmark data not found. Charts will not include benchmark comparison.")
    
    # Process each year
    all_results = []
    for year in years:
        try:
            result = calculate_portfolio_returns(year, stock_dict)
            if result:
                all_results.append(result)
                create_returns_chart(result, RETURNS_CHARTS_DIR, benchmark_data)
                
                # Save results to JSON
                os.makedirs(RETURNS_JSONS_DIR, exist_ok=True)
                results_file = os.path.join(RETURNS_JSONS_DIR, f'glassdoor_{year}_returns.json')
                with open(results_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                print(f"  Results saved: {results_file}")
                
                # Save individual stock returns to separate JSON
                if result.get('individual_stock_returns'):
                    stock_returns_file = os.path.join(RETURNS_JSONS_DIR, f'glassdoor_{year}_stock_returns.json')
                    stock_returns_data = {
                        'year': year,
                        'num_stocks': len(result['individual_stock_returns']),
                        'stocks': result['individual_stock_returns']
                    }
                    with open(stock_returns_file, 'w', encoding='utf-8') as f:
                        json.dump(stock_returns_data, f, indent=2, ensure_ascii=False)
                    print(f"  Stock returns saved: {stock_returns_file}")
        except Exception as e:
            print(f"\nError processing {year}: {e}")
            import traceback
            traceback.print_exc()
    
    if len(all_results) > 1:
        # Create summary chart
        create_summary_chart(all_results, RETURNS_CHARTS_DIR)
        
        # Create benchmark beat chart
        if benchmark_data:
            create_benchmark_beat_chart(all_results, benchmark_data, RETURNS_CHARTS_DIR)
        
        print(f"\n{'='*60}")
        print(f"Completed processing {len(all_results)} years")
        print(f"{'='*60}")


if __name__ == '__main__':
    main()

