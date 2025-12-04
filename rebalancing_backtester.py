"""
Rebalancing Portfolio Backtest: Annual Rebalancing Based on Metric Values

This script:
1. Loads stocks and calculates initial metrics
2. Stores initial revenue values for base weighting
3. Each year, recalculates metrics and rebalances the metric-weighted portfolio
4. Revenue-weighted portfolio stays static (no rebalancing)
5. Shows portfolio performance chart
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

# Import all the helper functions from backtester.py
# We'll copy them here for independence

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
    
    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m",
        "%Y",
    ]
    
    for fmt in formats:
        try:
            if fmt == "%Y-%m":
                if len(date_str) >= 7 and date_str[4] == '-' and date_str[6] in '0123456789':
                    return datetime.strptime(date_str[:7], fmt)
            elif fmt == "%Y":
                if len(date_str) >= 4:
                    return datetime.strptime(date_str[:4], fmt)
            else:
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
            if not allow_earlier and date_obj.year < target_year:
                continue
            
            diff = abs((date_obj - target_date).days)
            if diff < min_diff:
                min_diff = diff
                best_idx = idx
    
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
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
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
    if not cost_of_goods_sold:
        cost_of_goods_sold = data.get("cogs", [])
    
    if not isinstance(revenue, list) or not isinstance(cost_of_goods_sold, list):
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
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
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
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
    if not isinstance(revenue, list) or len(revenue) < 21:
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 20:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 20 and idx < len(period_dates) and idx < len(revenue):
            current_revenue = revenue[idx]
            revenue_5y_ago = revenue[idx - 20]
            
            if (current_revenue is not None and revenue_5y_ago is not None and
                current_revenue > 0 and revenue_5y_ago > 0):
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
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
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
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
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
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 19:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 19 and idx < len(period_dates) and idx < len(price_to_sales):
            current_ps = price_to_sales[idx]
            if current_ps is None or current_ps <= 0:
                continue
            
            ps_values = []
            for k in range(idx - 19, idx + 1):
                if k < len(price_to_sales) and price_to_sales[k] is not None:
                    ps_val = price_to_sales[k]
                    if ps_val is not None and ps_val > 0:
                        ps_values.append(float(ps_val))
            
            if len(ps_values) > 0:
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
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
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
    
    if start_date_str:
        start_idx = None
        for idx, date_str in enumerate(period_dates):
            if date_str == start_date_str:
                start_idx = idx
                break
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
    shares = 1.0
    
    for i in range(start_idx, len(period_dates)):
        if i >= len(prices) or prices[i] is None:
            continue
        
        current_price = float(prices[i])
        if current_price <= 0:
            continue
        
        dividend = float(dividends[i]) if i < len(dividends) and dividends[i] is not None else 0.0
        
        if dividend > 0:
            shares += dividend * shares / current_price
        
        current_value = shares * current_price
        cumulative_return = (current_value / start_price - 1.0) * 100
        
        date_str = period_dates[i]
        date_obj = parse_date(date_str)
        if date_obj:
            returns.append((date_obj, cumulative_return))
    
    return returns if returns else None

def get_metric_at_date(stock_data: Dict, metric_name: str, target_year: int) -> Optional[Tuple[float, str]]:
    """Get a metric value for a stock at a specific year"""
    metric_functions = {
        'ebit_ppe': get_ebit_ppe_at_date,
        'operating_margin': get_operating_margin_at_date,
        'gross_margin': get_gross_margin_at_date,
        '5y_revenue_cagr': get_5y_revenue_cagr_at_date,
        'ev_to_ebit': get_ev_to_ebit_at_date,
        'roa': get_roa_at_date,
        'relative_ps': get_relative_ps_at_date,
    }
    
    func = metric_functions.get(metric_name)
    if func:
        return func(stock_data, target_year)
    return None

def calculate_weights_for_period(stocks: List[Dict], metric_name: str, initial_revenue_total: float) -> Dict[str, float]:
    """
    Calculate metric-weighted portfolio weights for a period
    Uses initial revenue for base weights, but new metric rankings
    Returns dict: ticker -> final_weight_pct
    """
    # Filter stocks that have the metric for this period
    stocks_with_metric = [s for s in stocks if s.get('current_metric_value') is not None]
    
    if not stocks_with_metric:
        return {}
    
    # Rank by metric value
    reverse_sort = metric_name not in ['ev_to_ebit', 'relative_ps']  # These are reverse (lower is better)
    
    if reverse_sort:
        stocks_with_metric.sort(key=lambda x: x['current_metric_value'], reverse=True)
    else:
        stocks_with_metric.sort(key=lambda x: x['current_metric_value'] if x['current_metric_value'] is not None else float('inf'))
    
    # Calculate initial weights based on initial revenue
    for stock in stocks_with_metric:
        stock['initial_weight'] = (stock['initial_revenue'] / initial_revenue_total) * 100
    
    # Apply multiplier based on rank (0.5 for worst, 2.0 for best)
    n_stocks = len(stocks_with_metric)
    for i, stock in enumerate(stocks_with_metric):
        if n_stocks > 1:
            multiplier = 2.0 - (i / (n_stocks - 1)) * 1.5  # 2.0 to 0.5
        else:
            multiplier = 1.0
        stock['multiplier'] = multiplier
        stock['adjusted_weight'] = stock['initial_weight'] * multiplier
    
    # Normalize weights to sum to 100%
    total_adjusted = sum(s['adjusted_weight'] for s in stocks_with_metric)
    weights = {}
    for stock in stocks_with_metric:
        if total_adjusted > 0:
            weights[stock['ticker']] = (stock['adjusted_weight'] / total_adjusted) * 100
        else:
            weights[stock['ticker']] = 0.0
    
    return weights

def run_rebalancing_backtest_for_metric(stock_info_base: List[Dict], selected_metric: str, metric_name: str, metric_display_name: str):
    """Run rebalancing backtest for a specific metric"""
    print("\n" + "=" * 80)
    print(f"Running REBALANCING backtest for: {metric_name}")
    print("=" * 80)
    
    # Determine start year based on metric requirements
    metrics_needing_5y_history = ["5y_revenue_cagr", "relative_ps"]
    start_year = 2007 if selected_metric in metrics_needing_5y_history else 2002
    
    # Map metric to its date key
    date_key_map = {
        'ebit_ppe': 'ebit_date',
        'operating_margin': 'om_date',
        'gross_margin': 'gm_date',
        '5y_revenue_cagr': 'cagr_date',
        'ev_to_ebit': 'ev_date',
        'roa': 'roa_date',
        'relative_ps': 'ps_date'
    }
    metric_date_key = date_key_map.get(selected_metric, 'revenue_date')
    
    # Filter stocks that have the selected metric at the start year
    # For 5y metrics, we need to verify the metric is available at 2007
    # We'll check all stocks from the base list, not just those that already have the metric
    stock_info = []
    for s in stock_info_base:
        # Check if metric is available at start_year (don't require it to be in base list)
        metric_result = get_metric_at_date(s['stock_data'], selected_metric, start_year)
        if metric_result:
            stock_copy = s.copy()
            stock_copy['ticker'] = stock_copy.get('ticker') or stock_copy.get('symbol')
            
            # Get revenue from start_year for initial revenue
            revenue_result = get_revenue_at_date(s['stock_data'], start_year)
            if revenue_result:
                revenue, revenue_date = revenue_result
                stock_copy['initial_revenue'] = revenue
                stock_copy['initial_revenue_date'] = revenue_date
            else:
                # Fallback to existing revenue if not available at start_year
                stock_copy['initial_revenue'] = s['revenue']
                stock_copy['initial_revenue_date'] = s.get('revenue_date')
            
            stock_info.append(stock_copy)
    
    if not stock_info:
        print(f"   Skipping {metric_name}: No stocks found with this metric data for {start_year}")
        return
    
    # Sort by initial revenue and take top 500 to ensure we use up to 500 stocks
    stock_info.sort(key=lambda x: x['initial_revenue'], reverse=True)
    stock_info = stock_info[:500]
    
    # Recalculate initial revenue total after filtering
    initial_revenue_total = sum(s['initial_revenue'] for s in stock_info)
    
    print(f"   Using {len(stock_info)} stocks with initial revenue total: ${initial_revenue_total/1e9:.2f}B")
    
    # Get all available dates from stock returns
    print(f"\n   Calculating returns and collecting dates...")
    all_dates = set()
    stock_returns_by_date = {}  # ticker -> {date: return_pct}
    
    for stock in stock_info:
        # Use metric-specific date or initial revenue date for start
        start_date = stock.get(metric_date_key) or stock.get('initial_revenue_date')
        returns = calculate_total_return_with_dividends(stock['stock_data'], start_year, start_date)
        if returns:
            ticker = stock['ticker']
            stock_returns_by_date[ticker] = {}
            for date, return_pct in returns:
                all_dates.add(date)
                stock_returns_by_date[ticker][date] = return_pct
    
    if not all_dates:
        print("   Error: No return data calculated")
        return
    
    sorted_dates = sorted(all_dates)
    print(f"   Found {len(sorted_dates)} dates from {sorted_dates[0].strftime('%Y-%m-%d')} to {sorted_dates[-1].strftime('%Y-%m-%d')}")
    
    # Group dates by year for rebalancing
    dates_by_year = defaultdict(list)
    for date in sorted_dates:
        dates_by_year[date.year].append(date)
    
    rebalance_years = sorted(dates_by_year.keys())
    print(f"   Will rebalance at years: {rebalance_years[:5]}...{rebalance_years[-5:] if len(rebalance_years) > 10 else rebalance_years}")
    
    # Calculate revenue-weighted portfolio weights (static, no rebalancing)
    revenue_weights = {}
    for stock in stock_info:
        ticker = stock['ticker']
        revenue_weights[ticker] = (stock['initial_revenue'] / initial_revenue_total) * 100
    
    # Track portfolio values
    cumulative_returns_metric_weighted = []
    cumulative_returns_revenue_weighted = []
    
    # Track current weights for metric portfolio (will be updated at rebalancing)
    current_metric_weights = {}
    
    # Process each date
    last_rebalance_year = None
    portfolio_value_metric = 1.0  # Start at 1.0
    portfolio_value_revenue = 1.0  # Start at 1.0
    
    for date in sorted_dates:
        current_year = date.year
        
        # Rebalance if we've entered a new year
        if last_rebalance_year is None or current_year > last_rebalance_year:
            # Recalculate metrics for all stocks at this year
            print(f"   Rebalancing at {current_year}...")
            
            stocks_with_metric = []
            for stock in stock_info:
                ticker = stock['ticker']
                metric_result = get_metric_at_date(stock['stock_data'], selected_metric, current_year)
                if metric_result:
                    metric_value, metric_date = metric_result
                    stock['current_metric_value'] = metric_value
                    stock['current_metric_date'] = metric_date
                    stocks_with_metric.append(stock)
                else:
                    stock['current_metric_value'] = None
            
            if stocks_with_metric:
                # Calculate new weights based on current metric rankings
                current_metric_weights = calculate_weights_for_period(
                    stocks_with_metric, selected_metric, initial_revenue_total
                )
                print(f"      Rebalanced {len(stocks_with_metric)} stocks with metric data")
            else:
                print(f"      Warning: No stocks with metric data at {current_year}, keeping previous weights")
            
            last_rebalance_year = current_year
        
        # Calculate portfolio values for this date
        # Separate into active and inactive stocks
        active_stocks_metric = []
        inactive_stocks_metric = []
        active_stocks_revenue = []
        inactive_stocks_revenue = []
        
        for stock in stock_info:
            ticker = stock['ticker']
            metric_weight = current_metric_weights.get(ticker, 0.0) / 100.0
            revenue_weight = revenue_weights.get(ticker, 0.0) / 100.0
            
            if ticker in stock_returns_by_date and date in stock_returns_by_date[ticker]:
                stock_return_pct = stock_returns_by_date[ticker][date]
                stock_value_multiplier = 1.0 + (stock_return_pct / 100.0)
                active_stocks_metric.append((ticker, metric_weight, stock_value_multiplier))
                active_stocks_revenue.append((ticker, revenue_weight, stock_value_multiplier))
            else:
                # Find last known value
                last_return = 0.0
                if ticker in stock_returns_by_date:
                    for prev_date, prev_return in stock_returns_by_date[ticker].items():
                        if prev_date <= date:
                            last_return = prev_return
                stock_value_multiplier = 1.0 + (last_return / 100.0)
                inactive_stocks_metric.append((ticker, metric_weight, stock_value_multiplier))
                inactive_stocks_revenue.append((ticker, revenue_weight, stock_value_multiplier))
        
        # Calculate portfolio values
        portfolio_value_metric = 0.0
        portfolio_value_revenue = 0.0
        
        # Add active stocks
        for _, weight, value_mult in active_stocks_metric:
            portfolio_value_metric += weight * value_mult
        for _, weight, value_mult in active_stocks_revenue:
            portfolio_value_revenue += weight * value_mult
        
        # Redistribute inactive stocks' value
        total_active_weight_metric = sum(w for _, w, _ in active_stocks_metric)
        total_active_weight_revenue = sum(w for _, w, _ in active_stocks_revenue)
        inactive_value_metric = sum(w * v for _, w, v in inactive_stocks_metric)
        inactive_value_revenue = sum(w * v for _, w, v in inactive_stocks_revenue)
        
        if total_active_weight_metric > 0 and len(active_stocks_metric) > 0:
            for _, weight, value_mult in active_stocks_metric:
                proportion = weight / total_active_weight_metric
                portfolio_value_metric += proportion * inactive_value_metric
        
        if total_active_weight_revenue > 0 and len(active_stocks_revenue) > 0:
            for _, weight, value_mult in active_stocks_revenue:
                proportion = weight / total_active_weight_revenue
                portfolio_value_revenue += proportion * inactive_value_revenue
        
        # Calculate cumulative returns
        cumulative_return_metric = (portfolio_value_metric - 1.0) * 100
        cumulative_return_revenue = (portfolio_value_revenue - 1.0) * 100
        cumulative_returns_metric_weighted.append(cumulative_return_metric)
        cumulative_returns_revenue_weighted.append(cumulative_return_revenue)
    
    print(f"\n   Calculated returns for {len(sorted_dates)} time periods")
    print(f"      Start date: {sorted_dates[0].strftime('%Y-%m-%d')}")
    print(f"      End date: {sorted_dates[-1].strftime('%Y-%m-%d')}")
    print(f"      {metric_name} Weighted (Rebalanced) Total return: {cumulative_returns_metric_weighted[-1]:.2f}%")
    print(f"      Revenue Weighted (Static) Total return: {cumulative_returns_revenue_weighted[-1]:.2f}%")
    
    # Calculate annualized returns
    start_date_obj = sorted_dates[0]
    end_date_obj = sorted_dates[-1]
    delta_years = (end_date_obj - start_date_obj).days / 365.25
    
    annualized_return_metric = ((1 + cumulative_returns_metric_weighted[-1] / 100) ** (1 / delta_years) - 1) * 100 if delta_years > 0 else 0.0
    annualized_return_revenue = ((1 + cumulative_returns_revenue_weighted[-1] / 100) ** (1 / delta_years) - 1) * 100 if delta_years > 0 else 0.0
    
    print(f"      {metric_name} Weighted (Rebalanced) Annualized return: {annualized_return_metric:.2f}%")
    print(f"      Revenue Weighted (Static) Annualized return: {annualized_return_revenue:.2f}%")
    
    # Create output folder
    output_folder = "rebalancing_backtest_results"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Create chart
    print(f"\n   Creating performance chart...")
    plt.figure(figsize=(14, 8))
    
    plt.plot(sorted_dates, cumulative_returns_metric_weighted, linewidth=2, 
             color='#2E86AB', label=f'{metric_name} Weighted (Rebalanced) ({annualized_return_metric:+.1f}% ann.)')
    plt.plot(sorted_dates, cumulative_returns_revenue_weighted, linewidth=2, 
             color='#A23B72', label=f'Revenue Weighted (Static) ({annualized_return_revenue:+.1f}% ann.)', linestyle='--')
    
    plt.title(f'Rebalancing Portfolio Performance Comparison ({start_year} - Present)\n'
              f'{metric_name} Weighted (Annual Rebalancing) vs Revenue Weighted (Static)',
              fontsize=14, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Cumulative Return (%)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper left', fontsize=11)
    
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    plt.gca().xaxis.set_major_locator(mdates.YearLocator(5))
    plt.xticks(rotation=45)
    plt.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    plt.tight_layout()
    
    metric_filename = selected_metric.replace('/', '_').replace(' ', '_').lower()
    chart_filename = f'{output_folder}/{metric_filename}_rebalancing_backtest.png'
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"      Chart saved to {chart_filename}")
    plt.close()
    
    print(f"\n   Completed rebalancing backtest for {metric_name}")

def main():
    """Main function"""
    start_time = time.time()
    
    print("=" * 80)
    print("REBALANCING Portfolio Backtest (Annual Rebalancing)")
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
    
    print(f"\nWill run rebalancing backtests for {len(all_metrics)} metrics:")
    for i, metric in enumerate(all_metrics, 1):
        print(f"  {i}. {metric['metric_name']}")
    
    print("\n1. Finding S&P 500-like stocks from 2002...")
    print("   (Using stocks with data from 2002, ranked by revenue)")
    
    # Load all stock data
    print("   Loading all stock data...")
    nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
    nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
    all_stocks_list = nyse_stocks + nasdaq_stocks
    
    print(f"   Loaded {len(all_stocks_list)} stocks")
    
    # Find stocks with data around 2002
    print("   Finding stocks with data from 2002 (2007 for metrics requiring 5 years of history)...")
    stocks_with_data = []
    
    for stock_data in all_stocks_list:
        revenue_result = None
        ebit_ppe_result = None
        operating_margin_result = None
        gross_margin_result = None
        cagr_5y_result = None
        ev_ebit_result = None
        roa_result = None
        relative_ps_result = None
        
        # Try years 2002-2003 for most metrics
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
            if revenue_result:
                break
        
        # Metrics requiring 5 years of past data use 2007-2008
        for year in range(2007, 2009):
            if not cagr_5y_result:
                cagr_5y_result = get_5y_revenue_cagr_at_date(stock_data, year)
            if not relative_ps_result:
                relative_ps_result = get_relative_ps_at_date(stock_data, year)
        
        # Need revenue at minimum
        if revenue_result and revenue_result[0] >= 100_000_000:  # $100M minimum
            revenue, revenue_date = revenue_result
            stock_entry = {
                'ticker': stock_data.get('symbol'),
                'stock_data': stock_data,
                'revenue': revenue,
                'revenue_date': revenue_date
            }
            
            if ebit_ppe_result:
                ebit_ppe, ebit_date = ebit_ppe_result
                stock_entry['ebit_ppe'] = ebit_ppe
                stock_entry['ebit_date'] = ebit_date
            if operating_margin_result:
                om, om_date = operating_margin_result
                stock_entry['operating_margin'] = om
                stock_entry['om_date'] = om_date
            if gross_margin_result:
                gm, gm_date = gross_margin_result
                stock_entry['gross_margin'] = gm
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
    
    # Sort by revenue for reference, but don't limit yet - let each metric function select appropriately
    stocks_with_data.sort(key=lambda x: x['revenue'], reverse=True)
    
    print(f"   Found {len(stocks_with_data)} stocks with data and revenue >= $100M")
    
    # Create output folder
    output_folder = "rebalancing_backtest_results"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"\n   Created output folder: {output_folder}/")
    else:
        print(f"\n   Using output folder: {output_folder}/")
    
    # Run rebalancing backtest for each metric
    # Pass the full list so each metric can filter and select top 500 by revenue at its start year
    print("\n2. Running rebalancing backtests for all metrics...")
    for metric in all_metrics:
        run_rebalancing_backtest_for_metric(stocks_with_data, metric['selected_metric'], metric['metric_name'], metric['metric_display_name'])
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("\n" + "=" * 80)
    print("All Rebalancing Backtests Complete!")
    print("=" * 80)
    
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

