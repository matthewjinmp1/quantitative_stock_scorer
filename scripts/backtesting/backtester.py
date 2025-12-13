"""
Portfolio Backtest: EBIT/PPE Weighted S&P 500 Portfolio (2000)

This script:
1. Loads S&P 500 tickers from 2000
2. Gets EBIT/PPE for each stock around 2000
3. Ranks stocks by EBIT/PPE
4. Adjusts market cap weights based on ranking (0.5x to 2.0x multiplier)
5. Calculates total returns with dividends reinvested
6. Shows portfolio performance chart from 2000 to present
"""
import json
import os
import math
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import defaultdict
import numpy as np

def load_data_from_jsonl(filename: str) -> List[Dict]:
    """Load stock data from JSONL file"""
    if not os.path.exists(filename):
        return []
    
    stocks = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    stock = json.loads(line)
                    stocks.append(stock)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    return stocks

def get_period_dates(data: Dict) -> Optional[List]:
    """Extract period dates from data dictionary"""
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            return data[date_key]
    return None

def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string to datetime object"""
    if not date_str or date_str == "-":
        return None
    
    # Try different date formats - order matters, try most specific first
    formats = [
        "%Y-%m-%d",      # 2000-01-15
        "%Y-%m-%dT%H:%M:%S",  # ISO format with time
        "%Y-%m-%d %H:%M:%S",  # Space separated
        "%Y-%m",         # 2000-03 (YYYY-MM)
        "%Y",            # 2000
    ]
    
    for fmt in formats:
        try:
            # For formats that might have extra characters, try to match exactly
            if fmt == "%Y-%m":  # Special handling for YYYY-MM
                if len(date_str) >= 7 and date_str[4] == '-' and date_str[6] in '0123456789':
                    return datetime.strptime(date_str[:7], fmt)
            elif fmt == "%Y":  # Special handling for YYYY
                if len(date_str) >= 4:
                    return datetime.strptime(date_str[:4], fmt)
            else:
                # For other formats, try to match the full format length
                if len(date_str) >= len(fmt):
                    return datetime.strptime(date_str[:len(fmt)], fmt)
        except (ValueError, IndexError):
            continue
    
    return None

def find_quarter_near_date(period_dates: List, target_year: int = 2000, allow_earlier: bool = True) -> Optional[int]:
    """Find the index of the quarter closest to the target year"""
    if not period_dates:
        return None
    
    target_date = datetime(target_year, 1, 1)
    best_idx = None
    min_diff = float('inf')
    
    for idx, date_str in enumerate(period_dates):
        date_obj = parse_date(date_str)
        if date_obj:
            # If allow_earlier is False, only consider dates >= target_year
            if not allow_earlier and date_obj.year < target_year:
                continue
            
            diff = abs((date_obj - target_date).days)
            if diff < min_diff:
                min_diff = diff
                best_idx = idx
    
    # If no exact match and allow_earlier, try to find earliest available date
    if best_idx is None and allow_earlier:
        for idx, date_str in enumerate(period_dates):
            date_obj = parse_date(date_str)
            if date_obj:
                best_idx = idx
                break
    
    return best_idx

def get_ebit_ppe_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get EBIT/PPE for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    ppe_net = data.get("ppe_net", [])
    
    if not isinstance(operating_income, list) or not isinstance(ppe_net, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates):
            if (idx < len(operating_income) and idx < len(ppe_net) and
                operating_income[idx] is not None and ppe_net[idx] is not None and
                ppe_net[idx] != 0):
                ebit_ppe = operating_income[idx] / ppe_net[idx]
                return (ebit_ppe, period_dates[idx])
    
    return None

def get_gross_margin_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Gross Margin for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    cost_of_goods_sold = data.get("cost_of_goods_sold", [])
    # Also try alternative key name
    if not cost_of_goods_sold:
        cost_of_goods_sold = data.get("cogs", [])
    
    if not isinstance(revenue, list) or not isinstance(cost_of_goods_sold, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates):
            if (idx < len(revenue) and idx < len(cost_of_goods_sold) and
                revenue[idx] is not None and cost_of_goods_sold[idx] is not None and
                revenue[idx] != 0):
                gross_margin = (revenue[idx] - cost_of_goods_sold[idx]) / revenue[idx]
                return (gross_margin, period_dates[idx])
    
    return None

def get_operating_margin_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Operating Margin for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    revenue = data.get("revenue", [])
    
    if not isinstance(operating_income, list) or not isinstance(revenue, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates):
            if (idx < len(operating_income) and idx < len(revenue) and
                operating_income[idx] is not None and revenue[idx] is not None and
                revenue[idx] != 0):
                operating_margin = operating_income[idx] / revenue[idx]
                return (operating_margin, period_dates[idx])
    
    return None

def get_5y_revenue_cagr_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get 5-Year Revenue CAGR for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    if not isinstance(revenue, list) or len(revenue) < 21:  # Need at least 21 quarters (20 + current)
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 20:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 20 and idx < len(period_dates) and idx < len(revenue):
            current_revenue = revenue[idx]
            revenue_5y_ago = revenue[idx - 20]
            
            if (current_revenue is not None and revenue_5y_ago is not None and
                current_revenue > 0 and revenue_5y_ago > 0):
                # Calculate CAGR: ((Ending Value / Beginning Value)^(1/5) - 1) * 100
                ratio = current_revenue / revenue_5y_ago
                cagr_5y = ((ratio ** (1.0 / 5.0)) - 1.0) * 100.0
                return (cagr_5y, period_dates[idx])
    
    return None

def get_ev_to_ebit_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get EV/EBIT for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    enterprise_value = data.get("enterprise_value", [])
    operating_income = data.get("operating_income", [])
    
    if not isinstance(enterprise_value, list) or not isinstance(operating_income, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates):
            if (idx < len(enterprise_value) and idx < len(operating_income) and
                enterprise_value[idx] is not None and operating_income[idx] is not None and
                operating_income[idx] != 0):
                ev_ebit = enterprise_value[idx] / operating_income[idx]
                return (ev_ebit, period_dates[idx])
    
    return None

def get_roa_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get ROA for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    roa = data.get("roa", [])
    if not isinstance(roa, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates) and idx < len(roa):
            if roa[idx] is not None:
                return (roa[idx], period_dates[idx])
    
    return None

def get_relative_ps_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Relative PS (Current PS / 5-Year Median PS) for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    price_to_sales = data.get("price_to_sales", [])
    if not isinstance(price_to_sales, list) or len(price_to_sales) < 20:
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 19:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 19 and idx < len(period_dates) and idx < len(price_to_sales):
            current_ps = price_to_sales[idx]
            if current_ps is None or current_ps <= 0:
                continue
            
            # Calculate 5-year median (20 quarters) of price_to_sales
            ps_values = []
            for k in range(idx - 19, idx + 1):  # Include current period
                if k < len(price_to_sales) and price_to_sales[k] is not None:
                    ps_val = price_to_sales[k]
                    if ps_val is not None and ps_val > 0:  # Only include positive values
                        ps_values.append(float(ps_val))
            
            if len(ps_values) > 0:
                # Calculate median
                sorted_ps = sorted(ps_values)
                n = len(sorted_ps)
                if n % 2 == 0:
                    median_ps = (sorted_ps[n//2 - 1] + sorted_ps[n//2]) / 2.0
                else:
                    median_ps = sorted_ps[n//2]
                
                if median_ps > 0:
                    relative_ps = current_ps / median_ps
                    return (relative_ps, period_dates[idx])
    
    return None

def get_revenue_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get revenue for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenues = data.get("revenue", [])
    if not isinstance(revenues, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates) and idx < len(revenues):
            if revenues[idx] is not None and revenues[idx] > 0:
                return (revenues[idx], period_dates[idx])
    
    return None

def calculate_total_return_with_dividends(stock_data: Dict, start_year: int = 2000, start_date_str: Optional[str] = None) -> Optional[List[Tuple[datetime, float]]]:
    """
    Calculate cumulative total return with dividends reinvested
    Returns list of (date, cumulative_return) tuples
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates:
        return None
    
    prices = data.get("period_end_price", [])
    dividends = data.get("dividends", [])
    
    if not isinstance(prices, list) or not isinstance(dividends, list):
        return None
    
    # Find starting index - use provided date string if available, otherwise use year
    if start_date_str:
        # Find exact date match first
        start_idx = None
        for idx, date_str in enumerate(period_dates):
            if date_str == start_date_str:
                start_idx = idx
                break
        # If no exact match, find nearest
        if start_idx is None:
            start_date_obj = parse_date(start_date_str)
            if start_date_obj:
                start_idx = find_quarter_near_date(period_dates, start_date_obj.year)
    else:
        start_idx = find_quarter_near_date(period_dates, start_year)
    
    if start_idx is None or start_idx >= len(prices) or prices[start_idx] is None:
        return None
    
    start_price = float(prices[start_idx])
    if start_price <= 0:
        return None
    
    returns = []
    shares = 1.0  # Start with 1 share
    
    for i in range(start_idx, len(period_dates)):
        if i >= len(prices) or prices[i] is None:
            continue
        
        current_price = float(prices[i])
        if current_price <= 0:
            continue
        
        # Get dividend for this period
        dividend = float(dividends[i]) if i < len(dividends) and dividends[i] is not None else 0.0
        
        # Reinvest dividend by buying more shares
        if dividend > 0:
            shares += dividend * shares / current_price
        
        # Calculate cumulative return
        current_value = shares * current_price
        cumulative_return = (current_value / start_price - 1.0) * 100  # As percentage
        
        date_str = period_dates[i]
        date_obj = parse_date(date_str)
        if date_obj:
            returns.append((date_obj, cumulative_return))
    
    return returns if returns else None

def create_comparison_chart(results: List[Dict], output_folder: str, chart_type: str):
    """Create a comparison chart showing excess returns vs benchmark"""
    if not results:
        print(f"   No summary data to create comparison chart")
        return
    
    # Sort by excess return (how much they beat the benchmark)
    results.sort(key=lambda x: x.get('excess_return', 0), reverse=True)
    
    metrics = [r['metric_name'] for r in results]
    excess_returns = [r.get('excess_return', 0) for r in results]
    metric_returns = [r.get('metric_weighted_annualized', 0) for r in results]
    benchmark_returns = [r.get('revenue_weighted_annualized', 0) for r in results]
    
    # Create the chart - more square aspect ratio
    fig, ax = plt.subplots(figsize=(10, 10))
    fig.patch.set_facecolor('white')
    
    # Create bar chart
    x_pos = np.arange(len(metrics))
    colors = ['#2d8659' if x > 0 else '#c44e52' for x in excess_returns]
    
    bars = ax.barh(x_pos, excess_returns, color=colors, alpha=0.85, edgecolor='white', linewidth=2, height=0.75)
    
    # Set x-axis limits first to accommodate labels with more padding
    x_range = max(excess_returns) - min(excess_returns) if excess_returns else 1.0
    x_padding = max(x_range * 0.25, 0.3)  # Increased padding, minimum 0.3
    x_min = min(excess_returns) - x_padding if excess_returns else -0.5
    x_max = max(excess_returns) + x_padding if excess_returns else 0.5
    ax.set_xlim(x_min, x_max)
    
    # Add value labels on bars with better positioning
    for i, (bar, val) in enumerate(zip(bars, excess_returns)):
        width = bar.get_width()
        # Position label further outside the bar with more spacing
        if width >= 0:
            label_x = width + (x_max - width) * 0.15 + 0.05
        else:
            label_x = width - (width - x_min) * 0.15 - 0.05
        ax.text(label_x, bar.get_y() + bar.get_height()/2, 
                f'{val:+.2f}%', 
                ha='left' if width >= 0 else 'right',
                va='center', fontweight='bold', fontsize=12, color='#333333')
    
    # Customize chart
    ax.set_yticks(x_pos)
    ax.set_yticklabels(metrics, fontsize=13, fontweight='medium', color='#222222')
    ax.set_xlabel('Excess Annualized Return vs Revenue-Weighted Benchmark (%)', 
                  fontsize=13, fontweight='bold', labelpad=12, color='#222222')
    ax.set_title(f'Metric Performance Comparison ({chart_type})\n'
                 f'Annualized Excess Return: Metric Portfolio vs Revenue-Weighted Benchmark',
                 fontsize=16, fontweight='bold', pad=20, color='#111111')
    
    # Zero line
    ax.axvline(x=0, color='#333333', linestyle='--', linewidth=2, alpha=0.5, zorder=0)
    
    # Grid
    ax.grid(True, alpha=0.15, axis='x', linestyle='--', linewidth=1, color='#999999')
    ax.set_axisbelow(True)
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#2d8659', alpha=0.85, label='Outperforms Benchmark', edgecolor='white', linewidth=2),
        Patch(facecolor='#c44e52', alpha=0.85, label='Underperforms Benchmark', edgecolor='white', linewidth=2)
    ]
    ax.legend(handles=legend_elements, loc='lower right', fontsize=11, framealpha=0.95, 
              edgecolor='#cccccc', frameon=True)
    
    # Remove top and right spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#dddddd')
    ax.spines['bottom'].set_color('#dddddd')
    ax.spines['left'].set_linewidth(1.5)
    ax.spines['bottom'].set_linewidth(1.5)
    
    # Set tick colors
    ax.tick_params(colors='#555555', which='both')
    
    plt.tight_layout()
    
    # Save chart
    chart_filename = os.path.join(output_folder, f'metric_comparison_{chart_type.lower()}.png')
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print(f"   Comparison chart saved to {chart_filename}")
    plt.close()

def load_stock_data_by_symbol(tickers: List[str]) -> Dict[str, Dict]:
    """Load all stock data and index by symbol"""
    print("Loading stock data from nyse_data.jsonl and nasdaq_data.jsonl...")
    
    nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
    nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
    
    all_stocks = nyse_stocks + nasdaq_stocks
    stock_dict = {}
    
    for stock in all_stocks:
        symbol = stock.get("symbol", "").upper()
        if symbol:
            stock_dict[symbol] = stock
    
    print(f"Loaded data for {len(stock_dict)} unique stocks")
    return stock_dict

def run_backtest_for_metric(stock_info_base: List[Dict], selected_metric: str, metric_name: str, metric_display_name: str):
    """Run backtest for a specific metric"""
    print("\n" + "=" * 80)
    print(f"Running backtest for: {metric_name}")
    print("=" * 80)
    
    # Determine start year based on metric requirements
    # Metrics requiring 5 years of past data start from 2007
    metrics_needing_5y_history = ["5y_revenue_cagr", "relative_ps"]
    start_year = 2007 if selected_metric in metrics_needing_5y_history else 2002
    
    # Filter stocks that have the selected metric
    stock_info = [s.copy() for s in stock_info_base if selected_metric in s]
    
    if not stock_info:
        print(f"   Skipping {metric_name}: No stocks found with this metric data for {start_year}")
        return
    
    # Track earliest year (use metric-specific date field)
    earliest_year_found = None
    date_key_map = {
        'ebit_ppe': 'ebit_date',
        'operating_margin': 'om_date',
        'gross_margin': 'gm_date',
        '5y_revenue_cagr': 'cagr_date',
        'ev_to_ebit': 'ev_date',
        'roa': 'roa_date',
        'relative_ps': 'ps_date'
    }
    date_key = date_key_map.get(selected_metric, 'revenue_date')
    
    for stock in stock_info:
        if date_key in stock:
            date_obj = parse_date(stock[date_key])
            if date_obj:
                if earliest_year_found is None or date_obj.year < earliest_year_found:
                    earliest_year_found = date_obj.year
    
    if earliest_year_found:
        print(f"   Using data from {len(stock_info)} stocks (earliest data from {earliest_year_found})")
    else:
        print(f"   Using data from {len(stock_info)} stocks")
    
    # Rank by selected metric
    print(f"\n   Ranking stocks by {metric_name}...")
    if selected_metric == "ebit_ppe":
        stock_info.sort(key=lambda x: x['ebit_ppe'], reverse=True)
        for stock in stock_info:
            stock['metric_value'] = stock['ebit_ppe']
    elif selected_metric == "operating_margin":
        stock_info.sort(key=lambda x: x['operating_margin'], reverse=True)
        for stock in stock_info:
            stock['metric_value'] = stock['operating_margin']
    elif selected_metric == "gross_margin":
        stock_info.sort(key=lambda x: x['gross_margin'], reverse=True)
        for stock in stock_info:
            stock['metric_value'] = stock['gross_margin']
    elif selected_metric == "5y_revenue_cagr":
        stock_info.sort(key=lambda x: x['5y_revenue_cagr'], reverse=True)
        for stock in stock_info:
            stock['metric_value'] = stock['5y_revenue_cagr']
    elif selected_metric == "ev_to_ebit":
        # EV/EBIT: lower is better (reverse sort)
        stock_info.sort(key=lambda x: x['ev_to_ebit'] if x['ev_to_ebit'] is not None else float('inf'))
        for stock in stock_info:
            stock['metric_value'] = stock['ev_to_ebit']
    elif selected_metric == "roa":
        stock_info.sort(key=lambda x: x['roa'], reverse=True)
        for stock in stock_info:
            stock['metric_value'] = stock['roa']
    elif selected_metric == "relative_ps":
        # Relative PS: lower is better (reverse sort)
        stock_info.sort(key=lambda x: x['relative_ps'] if x['relative_ps'] is not None else float('inf'))
        for stock in stock_info:
            stock['metric_value'] = stock['relative_ps']
    
    # Calculate initial revenue weights
    total_revenue = sum(s['revenue'] for s in stock_info)
    for stock in stock_info:
        stock['initial_weight'] = (stock['revenue'] / total_revenue) * 100
    
    # Apply multiplier based on rank (0.5 for worst, 2.0 for best)
    print(f"\n   Applying {metric_name}-based weight adjustments...")
    n_stocks = len(stock_info)
    
    # For metrics where lower is better (EV/EBIT, Relative PS), we sorted ascending (lowest first = best)
    # For normal metrics, we sorted descending (highest first = best)
    # In both cases, index 0 is best, so we use the same multiplier formula
    for i, stock in enumerate(stock_info):
        # Linear interpolation: rank 0 (best) gets 2.0, rank n-1 (worst) gets 0.5
        if n_stocks > 1:
            multiplier = 2.0 - (i / (n_stocks - 1)) * 1.5  # 2.0 to 0.5
        else:
            multiplier = 1.0
        
        stock['multiplier'] = multiplier
        stock['adjusted_weight'] = stock['initial_weight'] * multiplier
    
    # Normalize weights to sum to 100%
    total_adjusted = sum(s['adjusted_weight'] for s in stock_info)
    for stock in stock_info:
        stock['final_weight'] = (stock['adjusted_weight'] / total_adjusted) * 100
    
    print(f"      Total adjusted weight before normalization: {total_adjusted:.2f}%")
    print(f"      Total final weight after normalization: {sum(s['final_weight'] for s in stock_info):.2f}%")
    
    # Show top 10 and bottom 10
    print(f"\n      Top 10 by {metric_name}:")
    for i, stock in enumerate(stock_info[:10], 1):
        metric_val = stock['metric_value']
        print(f"   {i:2d}. {stock['ticker']:6s} - {metric_display_name}: {metric_val:8.4f}, "
              f"Revenue: ${stock['revenue']/1e9:6.2f}B, "
              f"Initial: {stock['initial_weight']:5.2f}%, "
              f"Final: {stock['final_weight']:5.2f}%")
    
    print(f"\n      Bottom 10 by {metric_name}:")
    for i, stock in enumerate(stock_info[-10:], n_stocks - 9):
        metric_val = stock['metric_value']
        print(f"   {i:2d}. {stock['ticker']:6s} - {metric_display_name}: {metric_val:8.4f}, "
              f"Revenue: ${stock['revenue']/1e9:6.2f}B, "
              f"Initial: {stock['initial_weight']:5.2f}%, "
              f"Final: {stock['final_weight']:5.2f}%")
    
    # Calculate total returns with dividends reinvested
    print(f"\n   Calculating total returns with dividends reinvested...")
    
    # Determine start year for return calculations
    metrics_needing_5y_history = ["5y_revenue_cagr", "relative_ps"]
    return_start_year = 2007 if selected_metric in metrics_needing_5y_history else 2002
    
    # Collect all return data by date
    all_dates = set()
    stock_returns_by_date = {}  # ticker -> {date: return_pct}
    
    for stock in stock_info:
        # Use the actual date from metric calculation as start date
        start_date = stock.get(date_key)
        returns = calculate_total_return_with_dividends(stock['stock_data'], return_start_year, start_date)
        if returns:
            ticker = stock['ticker']
            stock_returns_by_date[ticker] = {}
            for date, return_pct in returns:
                all_dates.add(date)
                stock_returns_by_date[ticker][date] = return_pct
    
    if not all_dates:
        print("   Error: No return data calculated")
        return
    
    # Sort dates and calculate weighted portfolio returns
    sorted_dates = sorted(all_dates)
    cumulative_returns_ebit_weighted = []
    cumulative_returns_market_cap = []
    
    for date in sorted_dates:
        # Separate stocks into active (have data) and inactive (disappeared)
        active_stocks_ebit = []  # (ticker, ebit_weight, mc_weight, value_multiplier)
        inactive_stocks_ebit = []  # (ticker, ebit_weight, mc_weight, value_multiplier)
        active_stocks_mc = []
        inactive_stocks_mc = []
        
        # First pass: categorize stocks and get their values
        for stock in stock_info:
            ticker = stock['ticker']
            ebit_weight = stock['final_weight'] / 100.0  # EBIT/PPE adjusted weight
            mc_weight = stock['initial_weight'] / 100.0   # Market cap weight
            
            if ticker in stock_returns_by_date and date in stock_returns_by_date[ticker]:
                # Stock is active - has data for this date
                stock_return_pct = stock_returns_by_date[ticker][date]
                stock_value_multiplier = 1.0 + (stock_return_pct / 100.0)
                active_stocks_ebit.append((ticker, ebit_weight, stock_value_multiplier))
                active_stocks_mc.append((ticker, mc_weight, stock_value_multiplier))
            else:
                # Stock is inactive - find last known value
                last_return = 0.0
                if ticker in stock_returns_by_date:
                    for prev_date, prev_return in stock_returns_by_date[ticker].items():
                        if prev_date <= date:
                            last_return = prev_return
                stock_value_multiplier = 1.0 + (last_return / 100.0)
                inactive_stocks_ebit.append((ticker, ebit_weight, stock_value_multiplier))
                inactive_stocks_mc.append((ticker, mc_weight, stock_value_multiplier))
        
        # Calculate total weight of active stocks for rebalancing
        total_active_weight_ebit = sum(w for _, w, _ in active_stocks_ebit)
        total_active_weight_mc = sum(w for _, w, _ in active_stocks_mc)
        
        # Calculate value from inactive stocks (to be redistributed)
        inactive_value_ebit = sum(w * v for _, w, v in inactive_stocks_ebit)
        inactive_value_mc = sum(w * v for _, w, v in inactive_stocks_mc)
        
        # Calculate portfolio value
        portfolio_value_ebit = 0.0
        portfolio_value_mc = 0.0
        
        # Add active stocks' values
        for _, weight, value_mult in active_stocks_ebit:
            portfolio_value_ebit += weight * value_mult
        for _, weight, value_mult in active_stocks_mc:
            portfolio_value_mc += weight * value_mult
        
        # Redistribute inactive stocks' value proportionally to active stocks
        if total_active_weight_ebit > 0 and len(active_stocks_ebit) > 0:
            # Redistribute proportionally based on current weights
            for _, weight, value_mult in active_stocks_ebit:
                # Proportion of this active stock's weight
                proportion = weight / total_active_weight_ebit
                # Add proportional share of inactive value
                portfolio_value_ebit += proportion * inactive_value_ebit
        
        if total_active_weight_mc > 0 and len(active_stocks_mc) > 0:
            # Redistribute proportionally based on current weights
            for _, weight, value_mult in active_stocks_mc:
                # Proportion of this active stock's weight
                proportion = weight / total_active_weight_mc
                # Add proportional share of inactive value
                portfolio_value_mc += proportion * inactive_value_mc
        
        # Calculate cumulative return percentages
        cumulative_return_ebit = (portfolio_value_ebit - 1.0) * 100
        cumulative_return_mc = (portfolio_value_mc - 1.0) * 100
        cumulative_returns_ebit_weighted.append(cumulative_return_ebit)
        cumulative_returns_market_cap.append(cumulative_return_mc)
    
    print(f"      Calculated returns for {len(sorted_dates)} time periods")
    print(f"      Start date: {sorted_dates[0].strftime('%Y-%m-%d')}")
    print(f"      End date: {sorted_dates[-1].strftime('%Y-%m-%d')}")
    print(f"      {metric_name} Weighted Total return: {cumulative_returns_ebit_weighted[-1]:.2f}%")
    print(f"      Revenue Weighted Total return: {cumulative_returns_market_cap[-1]:.2f}%")
    
    # Create output folder for graphs
    output_folder = "backtest_results"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Calculate annualized returns for legend
    start_date = sorted_dates[0]
    end_date = sorted_dates[-1]
    years = (end_date - start_date).days / 365.25
    
    metric_weighted_total_return = cumulative_returns_ebit_weighted[-1]
    revenue_weighted_total_return = cumulative_returns_market_cap[-1]
    
    # Annualized return = ((1 + total_return/100)^(1/years) - 1) * 100
    if years > 0:
        metric_weighted_annualized = ((1 + metric_weighted_total_return / 100) ** (1 / years) - 1) * 100
        revenue_weighted_annualized = ((1 + revenue_weighted_total_return / 100) ** (1 / years) - 1) * 100
    else:
        metric_weighted_annualized = 0.0
        revenue_weighted_annualized = 0.0
    
    print(f"      {metric_name} Weighted Annualized return: {metric_weighted_annualized:.2f}%")
    print(f"      Revenue Weighted Annualized return: {revenue_weighted_annualized:.2f}%")
    
    # Create chart
    print(f"\n   Creating performance chart...")
    plt.figure(figsize=(14, 8))
    
    # Plot both lines with annualized returns in labels
    plt.plot(sorted_dates, cumulative_returns_ebit_weighted, linewidth=2, 
             color='#2E86AB', label=f'{metric_name} Weighted Portfolio ({metric_weighted_annualized:+.1f}% ann.)')
    plt.plot(sorted_dates, cumulative_returns_market_cap, linewidth=2, 
             color='#A23B72', label=f'Revenue Weighted Portfolio ({revenue_weighted_annualized:+.1f}% ann.)', linestyle='--')
    
    # Determine start year for title
    metrics_needing_5y_history = ["5y_revenue_cagr", "relative_ps"]
    title_start_year = 2007 if selected_metric in metrics_needing_5y_history else 2002
    
    plt.title(f'Portfolio Performance Comparison ({title_start_year} - Present)\n'
              f'{metric_name} Weighted vs Revenue Weighted S&P 500 with Dividends Reinvested',
              fontsize=14, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Cumulative Return (%)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper left', fontsize=11)
    
    # Format x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.gca().xaxis.set_major_locator(mdates.YearLocator(5))
    plt.xticks(rotation=45)
    
    # Add zero line
    plt.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    
    plt.tight_layout()
    
    # Create filename based on selected metric
    metric_filename = selected_metric.replace('/', '_').replace(' ', '_').lower()
    chart_filename = f'{output_folder}/{metric_filename}_portfolio_backtest.png'
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"      Chart saved to {chart_filename}")
    plt.close()  # Close figure instead of showing
    
    # Return summary data for comparison chart (don't save to file)
    summary_data = {
        'metric': selected_metric,
        'metric_name': metric_name,
        'start_year': title_start_year,
        'start_date': sorted_dates[0].strftime('%Y-%m-%d'),
        'end_date': sorted_dates[-1].strftime('%Y-%m-%d'),
        'metric_weighted_annualized': metric_weighted_annualized,
        'revenue_weighted_annualized': revenue_weighted_annualized,
        'excess_return': metric_weighted_annualized - revenue_weighted_annualized
    }
    
    print(f"\n   Completed backtest for {metric_name}")
    return summary_data

def main():
    """Main function"""
    start_time = time.time()
    
    print("=" * 80)
    print("Metric-Weighted Portfolio Backtest (Starting 2002)")
    print("Running all metrics automatically...")
    print("=" * 80)
    
    # Define all metrics
    all_metrics = [
        {"selected_metric": "ebit_ppe", "metric_name": "EBIT/PPE", "metric_display_name": "EBIT/PPE"},
        {"selected_metric": "operating_margin", "metric_name": "Operating Margin", "metric_display_name": "Operating Margin"},
        {"selected_metric": "gross_margin", "metric_name": "Gross Margin", "metric_display_name": "Gross Margin"},
        {"selected_metric": "5y_revenue_cagr", "metric_name": "5-Year Revenue CAGR", "metric_display_name": "5Y Rev CAGR"},
        {"selected_metric": "ev_to_ebit", "metric_name": "EV/EBIT", "metric_display_name": "EV/EBIT"},
        {"selected_metric": "roa", "metric_name": "ROA", "metric_display_name": "ROA"},
        {"selected_metric": "relative_ps", "metric_name": "Relative PS", "metric_display_name": "Relative PS"},
    ]
    
    print(f"\nWill run backtests for {len(all_metrics)} metrics:")
    for i, metric in enumerate(all_metrics, 1):
        print(f"  {i}. {metric['metric_name']}")
    
    # Instead of using current S&P 500 list, we'll find stocks that:
    # 1. Have data available around 2002 (when data coverage significantly increases)
    # 2. Were large cap at that time (top 500 by revenue)
    # This approximates the S&P 500 at that time
    
    print("\n1. Finding S&P 500-like stocks from 2002...")
    print("   (Using stocks with data from 2002, ranked by revenue)")
    print("   (2002 is when data coverage significantly increases)")
    
    # Load ALL stock data first
    print("   Loading all stock data...")
    all_stocks_list = load_data_from_jsonl("nyse_data.jsonl") + load_data_from_jsonl("nasdaq_data.jsonl")
    print(f"   Loaded {len(all_stocks_list)} stocks")
    
    # Find stocks with data around 2002 and get their revenue and metrics
    # Note: Metrics requiring 5 years of past data (5y_revenue_cagr, relative_ps) 
    # will use 2007 as start year instead of 2002
    print("   Finding stocks with data from 2002 (2007 for metrics requiring 5 years of history)...")
    stocks_with_data = []
    
    for stock_data in all_stocks_list:
        # Try to get revenue and all metrics
        # Most metrics use 2002, but metrics needing 5 years of history use 2007
        revenue_result = None
        ebit_ppe_result = None
        operating_margin_result = None
        gross_margin_result = None
        cagr_5y_result = None
        ev_ebit_result = None
        roa_result = None
        relative_ps_result = None
        
        # Try years 2002-2003 for most metrics (focus on 2002 when data coverage jumps)
        for year in range(2002, 2004):
            if not revenue_result:
                revenue_result = get_revenue_at_date(stock_data, year)
            if not ebit_ppe_result:
                ebit_ppe_result = get_ebit_ppe_at_date(stock_data, year)
            if not operating_margin_result:
                operating_margin_result = get_operating_margin_at_date(stock_data, year)
            if not gross_margin_result:
                gross_margin_result = get_gross_margin_at_date(stock_data, year)
            if not ev_ebit_result:
                ev_ebit_result = get_ev_to_ebit_at_date(stock_data, year)
            if not roa_result:
                roa_result = get_roa_at_date(stock_data, year)
            # We need revenue, so break once we have it
            if revenue_result:
                break
        
        # Metrics requiring 5 years of past data use 2007-2008 instead
        for year in range(2007, 2009):
            if not cagr_5y_result:
                cagr_5y_result = get_5y_revenue_cagr_at_date(stock_data, year)
            if not relative_ps_result:
                relative_ps_result = get_relative_ps_at_date(stock_data, year)
            if cagr_5y_result and relative_ps_result:
                break
        
        if revenue_result:
            revenue, rev_date = revenue_result
            
            # Only include stocks with meaningful revenue (at least $100M to approximate S&P 500)
            if revenue >= 100_000_000:  # $100M minimum
                stock_entry = {
                    'stock_data': stock_data,
                    'ticker': stock_data.get("symbol", "").upper(),
                    'revenue': revenue,
                    'revenue_date': rev_date,
                }
                
                # Add all metrics if available
                if ebit_ppe_result:
                    ebit_ppe, ebit_date = ebit_ppe_result
                    stock_entry['ebit_ppe'] = ebit_ppe
                    stock_entry['ebit_date'] = ebit_date
                if operating_margin_result:
                    operating_margin, om_date = operating_margin_result
                    stock_entry['operating_margin'] = operating_margin
                    stock_entry['om_date'] = om_date
                if gross_margin_result:
                    gross_margin, gm_date = gross_margin_result
                    stock_entry['gross_margin'] = gross_margin
                    stock_entry['gm_date'] = gm_date
                if cagr_5y_result:
                    cagr_5y, cagr_date = cagr_5y_result
                    stock_entry['5y_revenue_cagr'] = cagr_5y
                    stock_entry['cagr_date'] = cagr_date
                if ev_ebit_result:
                    ev_ebit, ev_date = ev_ebit_result
                    stock_entry['ev_to_ebit'] = ev_ebit
                    stock_entry['ev_date'] = ev_date
                if roa_result:
                    roa, roa_date = roa_result
                    stock_entry['roa'] = roa
                    stock_entry['roa_date'] = roa_date
                if relative_ps_result:
                    relative_ps, ps_date = relative_ps_result
                    stock_entry['relative_ps'] = relative_ps
                    stock_entry['ps_date'] = ps_date
                
                stocks_with_data.append(stock_entry)
    
    print(f"   Found {len(stocks_with_data)} stocks with data and revenue >= $100M")
    
    # Sort by revenue and take top 500 (approximate S&P 500)
    stocks_with_data.sort(key=lambda x: x['revenue'], reverse=True)
    stock_info = stocks_with_data[:500]
    
    print(f"   Selected top {len(stock_info)} stocks by revenue (S&P 500 approximation)")
    
    # Create output folder for graphs
    output_folder = "backtest_results"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"\n   Created output folder: {output_folder}/")
    else:
        print(f"\n   Using output folder: {output_folder}/")
    
    # Run backtest for each metric and collect summary data
    print("\n2. Running backtests for all metrics...")
    summary_results = []
    for metric in all_metrics:
        summary_data = run_backtest_for_metric(stock_info, metric['selected_metric'], metric['metric_name'], metric['metric_display_name'])
        if summary_data:
            summary_results.append(summary_data)
    
    # Create comparison chart
    if summary_results:
        print("\n3. Creating metric comparison chart...")
        output_folder = "backtest_results"
        create_comparison_chart(summary_results, output_folder, "Static")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("\n" + "=" * 80)
    print("All Backtests Complete!")
    print("=" * 80)
    
    # Print timing information
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)
    
    if hours > 0:
        print(f"\nTotal execution time: {hours}h {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")
    elif minutes > 0:
        print(f"\nTotal execution time: {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")
    else:
        print(f"\nTotal execution time: {seconds}s ({elapsed_time:.2f} seconds)")
    print("=" * 80)

if __name__ == "__main__":
    main()

