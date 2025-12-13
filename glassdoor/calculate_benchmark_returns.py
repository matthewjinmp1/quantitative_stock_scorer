"""
Calculate a S&P 500-like benchmark using the largest 500 stocks by revenue.

This script constructs a market-cap-weighted benchmark by:
1. Finding the 500 largest stocks by revenue for each year
2. Calculating monthly returns
3. Producing a total return chart from 2002 to present
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

# Project paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
GLASSDOOR_DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
BENCHMARK_DIR = os.path.join(GLASSDOOR_DATA_DIR, 'benchmark')


def parse_date_to_datetime(date_str: str) -> Optional[datetime]:
    """Parse date string to datetime object."""
    if not date_str or date_str == '-':
        return None
    
    # Try YYYY-MM-DD format
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        pass
    
    # Try YYYY-MM format
    try:
        return datetime.strptime(date_str, '%Y-%m')
    except (ValueError, TypeError):
        pass
    
    # Try FY2009.FQ1 format
    match = re.match(r'FY(\d{4})\.FQ(\d)', str(date_str))
    if match:
        year = int(match.group(1))
        quarter = int(match.group(2))
        month = (quarter - 1) * 3 + 1
        return datetime(year, month, 1)
    
    return None


def get_period_dates(data: Dict) -> List[str]:
    """Get list of period dates from stock data."""
    return data.get('period_end_date', [])


def load_stock_data() -> Dict[str, Dict]:
    """Load all stock data from NYSE and NASDAQ files."""
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


def get_revenue_for_year(stock: Dict, year: int) -> Optional[float]:
    """Get the revenue for a stock in a given year."""
    data = stock.get('data', {})
    period_dates = get_period_dates(data)
    revenues = data.get('revenue', [])
    
    if not period_dates or not revenues:
        return None
    
    # Find revenues for the target year
    year_revenues = []
    for idx, date_str in enumerate(period_dates):
        if idx >= len(revenues):
            continue
        
        date_obj = parse_date_to_datetime(date_str)
        if date_obj and date_obj.year == year:
            rev = revenues[idx]
            if rev is not None and rev > 0:
                year_revenues.append(rev)
    
    # Return the max revenue for that year (annual or sum of quarters)
    if year_revenues:
        return max(year_revenues)
    
    return None


def get_price_at_date(stock: Dict, target_date: datetime) -> Optional[float]:
    """Get the price at or before a target date."""
    data = stock.get('data', {})
    period_dates = get_period_dates(data)
    prices = data.get('period_end_price', [])
    
    if not period_dates or not prices:
        return None
    
    # Find price at or before target date
    best_price = None
    best_date = None
    
    for idx, date_str in enumerate(period_dates):
        if idx >= len(prices):
            continue
        
        price = prices[idx]
        if price is None or price <= 0:
            continue
        
        date_obj = parse_date_to_datetime(date_str)
        if date_obj is None:
            continue
        
        if date_obj <= target_date:
            if best_date is None or date_obj > best_date:
                best_date = date_obj
                best_price = price
    
    return best_price


def get_largest_stocks_by_revenue(stock_dict: Dict[str, Dict], year: int, top_n: int = 500) -> List[Tuple[str, float]]:
    """Get the top N stocks by revenue for a given year."""
    revenues = []
    
    for ticker, stock in stock_dict.items():
        revenue = get_revenue_for_year(stock, year)
        if revenue is not None and revenue > 0:
            revenues.append((ticker, revenue))
    
    # Sort by revenue descending and take top N
    revenues.sort(key=lambda x: x[1], reverse=True)
    return revenues[:top_n]


def calculate_benchmark_returns(stock_dict: Dict[str, Dict], start_year: int = 2002) -> Dict:
    """
    Calculate benchmark returns from start_year to present.
    
    Rebalances annually to include the top 500 stocks by revenue.
    Tracks monthly portfolio values.
    """
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    print(f"\nCalculating benchmark returns from {start_year} to {current_year}...")
    
    # Initial investment
    initial_value = 100000.0  # $100k initial investment
    portfolio_value = initial_value
    
    # Track portfolio over time
    portfolio_values = []  # List of (date, value) tuples
    
    # Track current holdings: ticker -> shares
    current_holdings = {}
    
    # Start from January of start_year
    start_date = datetime(start_year, 1, 1)
    
    # Process each year
    for year in range(start_year, current_year + 1):
        print(f"\n  Processing {year}...")
        
        # Get top 500 stocks by revenue for this year
        # Use previous year's revenue to select stocks (more realistic)
        selection_year = year - 1 if year > start_year else year
        top_stocks = get_largest_stocks_by_revenue(stock_dict, selection_year, 500)
        
        if not top_stocks:
            print(f"    Warning: No stocks found for {year}, using previous holdings")
            continue
        
        print(f"    Found {len(top_stocks)} stocks by revenue")
        
        # Get stocks that have price data for this year
        valid_stocks = []
        for ticker, revenue in top_stocks:
            price = get_price_at_date(stock_dict[ticker], datetime(year, 1, 1))
            if price is not None and price > 0:
                valid_stocks.append((ticker, revenue, price))
        
        print(f"    {len(valid_stocks)} stocks have price data")
        
        if not valid_stocks:
            continue
        
        # Calculate total revenue for weighting
        total_revenue = sum(rev for _, rev, _ in valid_stocks)
        
        # Rebalance portfolio at start of year
        # Equal-weight for simplicity (or revenue-weighted)
        per_stock_value = portfolio_value / len(valid_stocks)
        
        new_holdings = {}
        for ticker, revenue, price in valid_stocks:
            shares = per_stock_value / price
            new_holdings[ticker] = shares
        
        current_holdings = new_holdings
        
        # Track monthly values throughout the year
        end_month = 12 if year < current_year else min(current_month, 12)
        
        for month in range(1, end_month + 1):
            month_date = datetime(year, month, 1)
            
            # Calculate portfolio value at this month
            month_value = 0.0
            stocks_with_data = 0
            
            for ticker, shares in current_holdings.items():
                if ticker not in stock_dict:
                    continue
                
                price = get_price_at_date(stock_dict[ticker], month_date)
                if price is not None and price > 0:
                    month_value += shares * price
                    stocks_with_data += 1
            
            if month_value > 0:
                portfolio_values.append((month_date, month_value))
                portfolio_value = month_value  # Update for next iteration
        
        if valid_stocks:
            print(f"    Year-end portfolio value: ${portfolio_value:,.2f}")
    
    # Calculate returns
    if not portfolio_values:
        return None
    
    final_value = portfolio_values[-1][1]
    total_return_pct = ((final_value - initial_value) / initial_value) * 100
    
    # Calculate annualized return
    first_date = portfolio_values[0][0]
    last_date = portfolio_values[-1][0]
    years_held = (last_date - first_date).days / 365.25
    
    if years_held > 0 and final_value > 0 and initial_value > 0:
        annualized_return = ((final_value / initial_value) ** (1 / years_held) - 1) * 100
    else:
        annualized_return = 0
    
    return {
        'start_year': start_year,
        'end_year': current_year,
        'initial_value': initial_value,
        'final_value': final_value,
        'total_return_pct': total_return_pct,
        'annualized_return_pct': annualized_return,
        'years': years_held,
        'portfolio_values': [(d.isoformat(), v) for d, v in portfolio_values]
    }


def create_benchmark_chart(results: Dict, output_dir: str):
    """Create a chart showing benchmark returns over time."""
    if not results or 'portfolio_values' not in results:
        return
    
    # Parse portfolio values
    portfolio_values = [(datetime.fromisoformat(d), v) for d, v in results['portfolio_values']]
    
    if len(portfolio_values) < 2:
        print("Not enough data points to create chart")
        return
    
    dates = [pv[0] for pv in portfolio_values]
    values = [pv[1] for pv in portfolio_values]
    
    # Calculate returns as percentage
    initial = results['initial_value']
    returns_pct = [(v - initial) / initial * 100 for v in values]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(16, 9))
    
    # Plot
    ax.plot(dates, returns_pct, linewidth=2, color='#1a5f7a', marker='o', markersize=1.5, alpha=0.9)
    ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
    ax.fill_between(dates, 0, returns_pct, alpha=0.3, color='#1a5f7a')
    
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Total Return (%)', fontsize=12)
    ax.set_title(f'Top 500 Stocks by Revenue - Total Returns ({results["start_year"]}-{results["end_year"]})\n(S&P 500-like Benchmark)', 
                 fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Format x-axis
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45, ha='right')
    
    # Add statistics text
    stats_text = f"Initial: ${results['initial_value']:,.0f} | "
    stats_text += f"Final: ${results['final_value']:,.0f}\n"
    stats_text += f"Total Return: {results['total_return_pct']:,.1f}% | "
    stats_text += f"Annualized: {results['annualized_return_pct']:.1f}%\n"
    stats_text += f"Period: {results['years']:.1f} years ({len(portfolio_values)} monthly data points)"
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    
    # Save chart
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'benchmark_top500_returns.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nBenchmark chart saved: {output_file}")


def main():
    """Main function."""
    # Fixed start year - 2003 has better data coverage
    START_YEAR = 2003
    
    # Load stock data
    print("Loading stock data from NYSE and NASDAQ files...")
    stock_dict = load_stock_data()
    print(f"Loaded {len(stock_dict)} stocks")
    
    if not stock_dict:
        print("Error: No stock data loaded")
        return
    
    # Calculate benchmark returns
    results = calculate_benchmark_returns(stock_dict, START_YEAR)
    
    if not results:
        print("Error: Could not calculate benchmark returns")
        return
    
    # Print summary
    print(f"\n{'='*60}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*60}")
    print(f"Period: {results['start_year']} - {results['end_year']} ({results['years']:.1f} years)")
    print(f"Initial Value: ${results['initial_value']:,.2f}")
    print(f"Final Value: ${results['final_value']:,.2f}")
    print(f"Total Return: {results['total_return_pct']:,.2f}%")
    print(f"Annualized Return: {results['annualized_return_pct']:.2f}%")
    print(f"Data Points: {len(results['portfolio_values'])} monthly values")
    print(f"{'='*60}")
    
    # Create output directory
    os.makedirs(BENCHMARK_DIR, exist_ok=True)
    
    # Create chart
    create_benchmark_chart(results, BENCHMARK_DIR)
    
    # Save JSON results
    json_file = os.path.join(BENCHMARK_DIR, 'benchmark_top500_returns.json')
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Results saved: {json_file}")


if __name__ == '__main__':
    main()

