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
import numpy as np

# ============================================================================
# METRIC CONFIGURATION - ADD NEW METRICS HERE
# ============================================================================
# To add a new metric:
# 1. Create the getter function (e.g., get_my_metric_at_date)
# 2. Add a MetricConfig entry to METRICS list below
# 3. That's it! The rest is automatic.

from dataclasses import dataclass
from typing import Callable

@dataclass
class MetricConfig:
    """Configuration for a single metric"""
    key: str  # Internal key name (e.g., "ebit_ppe")
    display_name: str  # Display name (e.g., "EBIT/PPE")
    short_name: str  # Short display name for charts (e.g., "EBIT/PPE")
    getter_function: Callable  # Function to get metric value at a date
    reverse_sort: bool = False  # True if lower values are better (e.g., EV/EBIT, consistency metrics)
    quarters_of_history_needed: int = 0  # Number of quarters of historical data needed (start year = 2002 + ceil(quarters/4))
    date_key: str = ""  # Key for storing date in stock_entry (auto-generated if empty)
    load_years: tuple = (2002, 2004)  # Year range to check when loading initial data
    
    @property
    def years_of_history_needed(self) -> int:
        """Calculate years needed from quarters (for backward compatibility)"""
        return math.ceil(self.quarters_of_history_needed / 4.0)

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

def get_5y_revenue_growth_rate_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get 5y halfway growth for a stock at a specific date
    Uses 20 quarters: sum of first 10 quarters vs sum of last 10 quarters
    growth = sum2 / sum1
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    if not isinstance(revenue, list) or len(revenue) < 20:
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 19:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 19 and idx < len(period_dates) and idx < len(revenue):
            # Sum last 10 quarters (quarters idx-9 to idx, inclusive)
            sum2 = 0.0
            valid_sum2 = True
            for i in range(idx - 9, idx + 1):
                if i < len(revenue) and revenue[i] is not None and revenue[i] > 0:
                    sum2 += revenue[i]
                else:
                    valid_sum2 = False
                    break
            
            if not valid_sum2 or sum2 <= 0:
                continue
            
            # Sum first 10 quarters (quarters idx-19 to idx-10, inclusive)
            sum1 = 0.0
            valid_sum1 = True
            for i in range(idx - 19, idx - 9):
                if i < len(revenue) and revenue[i] is not None and revenue[i] > 0:
                    sum1 += revenue[i]
                else:
                    valid_sum1 = False
                    break
            
            if valid_sum1 and sum1 > 0:
                growth = sum2 / sum1
                return (growth, period_dates[idx])
    
    return None

def get_5y_share_growth_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get 5y share growth for a stock at a specific date
    Uses 20 quarters: sum of first 10 quarters vs sum of last 10 quarters
    growth = sum2 / sum1
    Lower is better (fewer shares is better)
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    shares = data.get("shares_diluted", [])
    if not isinstance(shares, list) or len(shares) < 20:
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 19:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 19 and idx < len(period_dates) and idx < len(shares):
            # Sum last 10 quarters (quarters idx-9 to idx, inclusive)
            sum2 = 0.0
            valid_sum2 = True
            for i in range(idx - 9, idx + 1):
                if i < len(shares) and shares[i] is not None and shares[i] > 0:
                    sum2 += shares[i]
                else:
                    valid_sum2 = False
                    break
            
            if not valid_sum2 or sum2 <= 0:
                continue
            
            # Sum first 10 quarters (quarters idx-19 to idx-10, inclusive)
            sum1 = 0.0
            valid_sum1 = True
            for i in range(idx - 19, idx - 9):
                if i < len(shares) and shares[i] is not None and shares[i] > 0:
                    sum1 += shares[i]
                else:
                    valid_sum1 = False
                    break
            
            if valid_sum1 and sum1 > 0:
                growth = sum2 / sum1
                return (growth, period_dates[idx])
    
    return None

def get_ttm_share_buyback_to_market_cap_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get TTM Share Buyback to Market Cap for a stock at a specific date
    Uses last 4 quarters (TTM):
    - shares_current = shares at current quarter
    - shares_4q_ago = shares 4 quarters ago
    - share_reduction = shares_4q_ago - shares_current (positive if buyback)
    - market_cap = market cap at current quarter
    - metric = share_reduction / market_cap
    Higher is better (more buybacks is better)
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    shares = data.get("shares_diluted", [])
    market_cap = data.get("market_cap", [])
    
    if not isinstance(shares, list) or not isinstance(market_cap, list):
        return None
    
    if len(shares) < 5 or len(market_cap) < 1:  # Need at least 5 quarters of shares data
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 4:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 4 and idx < len(period_dates) and idx < len(shares) and idx < len(market_cap):
            # Get shares at current quarter and 4 quarters ago
            shares_current = shares[idx] if idx < len(shares) else None
            shares_4q_ago = shares[idx - 4] if (idx - 4) >= 0 and (idx - 4) < len(shares) else None
            market_cap_current = market_cap[idx] if idx < len(market_cap) else None
            
            if (shares_current is not None and shares_4q_ago is not None and 
                market_cap_current is not None and 
                shares_current > 0 and shares_4q_ago > 0 and market_cap_current > 0):
                
                # Calculate share reduction (positive if buyback, negative if dilution)
                share_reduction = shares_4q_ago - shares_current
                
                # Calculate metric: share buyback as fraction of market cap
                # This represents the percentage of market cap that was used for buybacks
                buyback_to_market_cap = share_reduction / market_cap_current
                
                return (buyback_to_market_cap, period_dates[idx])
    
    return None

def get_acceleration_of_growth_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Acceleration of Growth for a stock at a specific date
    Uses 21 quarters:
    - sum1 = revenue of quarters 1-7 (indices idx-20 to idx-14)
    - sum2 = revenue of quarters 8-14 (indices idx-13 to idx-7)
    - sum3 = revenue of quarters 15-21 (indices idx-6 to idx)
    acceleration = (sum3 / sum2) / (sum2 / sum1)
    """
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
            # Sum quarters 1-7 (indices idx-20 to idx-14, inclusive)
            sum1 = 0.0
            valid_sum1 = True
            for i in range(idx - 20, idx - 13):
                if i < len(revenue) and revenue[i] is not None and revenue[i] > 0:
                    sum1 += revenue[i]
                else:
                    valid_sum1 = False
                    break
            
            if not valid_sum1 or sum1 <= 0:
                continue
            
            # Sum quarters 8-14 (indices idx-13 to idx-7, inclusive)
            sum2 = 0.0
            valid_sum2 = True
            for i in range(idx - 13, idx - 6):
                if i < len(revenue) and revenue[i] is not None and revenue[i] > 0:
                    sum2 += revenue[i]
                else:
                    valid_sum2 = False
                    break
            
            if not valid_sum2 or sum2 <= 0:
                continue
            
            # Sum quarters 15-21 (indices idx-6 to idx, inclusive)
            sum3 = 0.0
            valid_sum3 = True
            for i in range(idx - 6, idx + 1):
                if i < len(revenue) and revenue[i] is not None and revenue[i] > 0:
                    sum3 += revenue[i]
                else:
                    valid_sum3 = False
                    break
            
            if valid_sum3 and sum3 > 0 and sum2 > 0:
                # acceleration = (sum3 / sum2) / (sum2 / sum1)
                growth_rate_2_to_3 = sum3 / sum2
                growth_rate_1_to_2 = sum2 / sum1
                if growth_rate_1_to_2 > 0:
                    acceleration = growth_rate_2_to_3 / growth_rate_1_to_2
                    return (acceleration, period_dates[idx])
    
    return None

def get_consistency_of_growth_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Consistency of Growth metric for a stock at a specific date
    Takes past 20 quarters of revenue, calculates YoY growth rates, then calculates stdev
    Lower stdev = more consistent growth (better)
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    if not isinstance(revenue, list) or len(revenue) < 20:
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 19:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 19 and idx < len(period_dates) and idx < len(revenue):
            # Calculate YoY growth rates for the past 20 quarters
            # We need quarters from idx-19 to idx (20 quarters total)
            # For each quarter from idx-15 to idx, calculate YoY growth (compare to 4 quarters ago)
            yoy_growth_rates = []
            
            for i in range(idx - 15, idx + 1):  # Need at least 4 quarters before to calculate YoY
                if i >= 4 and i < len(revenue):
                    current_rev = revenue[i]
                    year_ago_rev = revenue[i - 4]
                    
                    if (current_rev is not None and year_ago_rev is not None and
                        current_rev > 0 and year_ago_rev > 0):
                        yoy_growth = (current_rev / year_ago_rev - 1.0) * 100.0  # As percentage
                        yoy_growth_rates.append(yoy_growth)
            
            # Need at least a few growth rates to calculate stdev
            if len(yoy_growth_rates) >= 4:
                # Calculate standard deviation
                mean_growth = sum(yoy_growth_rates) / len(yoy_growth_rates)
                variance = sum((x - mean_growth) ** 2 for x in yoy_growth_rates) / len(yoy_growth_rates)
                stdev = math.sqrt(variance)
                return (stdev, period_dates[idx])
    
    return None

def get_consistency_of_operating_margin_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Consistency of Operating Margin metric for a stock at a specific date
    Takes past 20 quarters of operating margins, calculates stdev
    Lower stdev = more consistent operating margins (better)
    """
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
    
    if len(operating_income) < 20 or len(revenue) < 20:
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 19:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 19 and idx < len(period_dates) and idx < len(operating_income) and idx < len(revenue):
            # Calculate operating margins for the past 20 quarters (idx-19 to idx)
            operating_margins = []
            
            for i in range(idx - 19, idx + 1):
                if i < len(operating_income) and i < len(revenue):
                    op_income = operating_income[i]
                    rev = revenue[i]
                    
                    if (op_income is not None and rev is not None and rev != 0):
                        operating_margin = op_income / rev
                        operating_margins.append(operating_margin)
            
            # Need at least a few operating margins to calculate stdev
            if len(operating_margins) >= 10:
                # Calculate standard deviation
                mean_margin = sum(operating_margins) / len(operating_margins)
                variance = sum((x - mean_margin) ** 2 for x in operating_margins) / len(operating_margins)
                stdev = math.sqrt(variance)
                return (stdev, period_dates[idx])
    
    return None

def get_dividend_yield_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Dividend Yield for a stock at a specific date
    Calculated as (trailing 4 quarters of dividends / current price) * 100
    Higher yield is better (not a reverse metric)
    Allows 0 dividends in some quarters as long as there's some dividend data
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    prices = data.get("period_end_price", [])
    dividends = data.get("dividends", [])
    
    if not isinstance(prices, list) or not isinstance(dividends, list):
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None or quarter_idx < 3:  # Need at least 4 quarters (0-3)
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if idx >= 3 and idx < len(period_dates) and idx < len(prices):
            current_price = prices[idx]
            if current_price is None or current_price <= 0:
                continue
            
            # Sum trailing 4 quarters of dividends (allow 0 dividends, but need at least some data)
            trailing_dividends = []
            for i in range(max(0, idx - 3), idx + 1):
                if i < len(dividends) and dividends[i] is not None:
                    # Include 0 dividends as well, but track separately
                    trailing_dividends.append(float(dividends[i]) if dividends[i] > 0 else 0.0)
            
            # Need all 4 quarters of data (even if some are 0), and at least some positive dividends
            if len(trailing_dividends) == 4 and sum(trailing_dividends) > 0:
                annual_dividends = sum(trailing_dividends)
                dividend_yield = (annual_dividends / float(current_price)) * 100.0  # As percentage
                return (dividend_yield, period_dates[idx])
    
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

def get_net_debt_to_ebit_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Net Debt to EBIT for a stock at a specific date
    Calculated as Net Debt / Operating Income (EBIT)
    Lower values are better (less debt relative to earnings)
    
    Requirements:
    - EBIT (operating_income) must be positive for meaningful ratio
    - Negative net_debt (net cash position) is allowed and results in negative ratio (favorable)
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    net_debt = data.get("net_debt", [])
    operating_income = data.get("operating_income", [])
    
    if not isinstance(net_debt, list) or not isinstance(operating_income, list):
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates):
            if (idx < len(net_debt) and idx < len(operating_income) and
                net_debt[idx] is not None and operating_income[idx] is not None and
                operating_income[idx] > 0):  # Require positive EBIT for meaningful ratio
                # Calculate Net Debt to EBIT
                # Note: net_debt can be negative (net cash position), which is fine
                # Negative net_debt with positive EBIT gives negative ratio (good - company has cash)
                net_debt_to_ebit = net_debt[idx] / operating_income[idx]
                return (net_debt_to_ebit, period_dates[idx])
    
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

def get_price_to_book_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Price to Book Value for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    price_to_book = data.get("price_to_book", [])
    if not isinstance(price_to_book, list):
        return None
    
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates) and idx < len(price_to_book):
            if price_to_book[idx] is not None and price_to_book[idx] > 0:
                return (price_to_book[idx], period_dates[idx])
    
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

def get_total_past_return_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Total Past Return (price + dividends) for a stock at a specific date
    Calculates cumulative return from the earliest available data point to the target date
    Uses all available historical data (not a fixed period)
    Higher return is better
    """
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    prices = data.get("period_end_price", [])
    dividends = data.get("dividends", [])
    
    if not isinstance(prices, list):
        return None
    # Dividends can be None or empty list - that's fine, we'll just use 0
    if dividends is None:
        dividends = []
    if not isinstance(dividends, list):
        dividends = []
    
    # Find the target date index
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try different offsets to find a valid end date
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 7, -7, 8, -8]:
        end_idx = quarter_idx + offset
        if end_idx < 0 or end_idx >= len(period_dates) or end_idx >= len(prices):
            continue
        
        # End price must be valid
        if prices[end_idx] is None:
            continue
        
        end_price = float(prices[end_idx])
        if end_price <= 0:
            continue
        
        # Find the earliest available price (start of data)
        start_idx = None
        for i in range(end_idx + 1):  # Search from beginning up to end_idx
            if i < len(prices) and prices[i] is not None:
                start_price_val = float(prices[i])
                if start_price_val > 0:
                    start_idx = i
                    break
        
        if start_idx is None or start_idx >= end_idx:
            continue  # Need at least 2 quarters of data
        
        start_price = float(prices[start_idx])
        
        # Calculate cumulative return with dividends reinvested
        shares = 1.0
        
        # Reinvest dividends from start_idx+1 to end_idx (allow missing dividend data - just use 0)
        for i in range(start_idx + 1, end_idx + 1):
            if i < len(dividends) and dividends[i] is not None:
                dividend = float(dividends[i]) if dividends[i] > 0 else 0.0
                # Use end price if price at dividend date is missing
                price_at_dividend = float(prices[i]) if (i < len(prices) and prices[i] is not None and prices[i] > 0) else end_price
                if dividend > 0 and price_at_dividend > 0:
                    shares += dividend * shares / price_at_dividend
        
        # Calculate final value
        current_value = shares * end_price
        cumulative_return = (current_value / start_price - 1.0) * 100.0  # As percentage
        
        # Annualize the return based on actual time period
        start_date_str = period_dates[start_idx]
        end_date_str = period_dates[end_idx]
        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)
        
        if start_date and end_date:
            # Calculate years between start and end
            days_diff = (end_date - start_date).days
            years = days_diff / 365.25
            
            if years > 0:
                # Annualize the return
                annualized_return = ((1.0 + cumulative_return / 100.0) ** (1.0 / years) - 1.0) * 100.0
                return (annualized_return, period_dates[end_idx])
            else:
                # Less than a year, return as-is
                return (cumulative_return, period_dates[end_idx])
        else:
            # Can't parse dates, assume quarterly and annualize
            quarters = end_idx - start_idx
            if quarters > 0:
                years = quarters / 4.0
                annualized_return = ((1.0 + cumulative_return / 100.0) ** (1.0 / years) - 1.0) * 100.0
                return (annualized_return, period_dates[end_idx])
    
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
    
    # Start returns from the NEXT period (t+1) to avoid look-ahead bias
    # The metric is calculated using data up to time t, so returns should start at t+1
    return_start_idx = start_idx + 1
    
    # If there's no next period, we can't calculate returns
    if return_start_idx >= len(period_dates) or return_start_idx >= len(prices):
        return None
    
    returns = []
    shares = 1.0
    
    for i in range(return_start_idx, len(period_dates)):
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

def load_cached_results(cache_file: str) -> List[Dict]:
    """Load cached backtest results from JSON file"""
    if not os.path.exists(cache_file):
        return []
    
    try:
        with open(cache_file, 'r') as f:
            results = json.load(f)
            return results if isinstance(results, list) else []
    except (json.JSONDecodeError, IOError) as e:
        print(f"   Warning: Could not load cache file {cache_file}: {e}")
        return []

def save_result_to_cache(cache_file: str, result: Dict):
    """Save or update a single result in the cache file"""
    # Load existing results
    existing_results = load_cached_results(cache_file)
    
    # Find if this metric already exists (by 'metric' key)
    metric_key = result.get('metric')
    updated = False
    for i, existing in enumerate(existing_results):
        if existing.get('metric') == metric_key:
            existing_results[i] = result  # Update existing
            updated = True
            break
    
    if not updated:
        existing_results.append(result)  # Add new
    
    # Save back to file
    try:
        with open(cache_file, 'w') as f:
            json.dump(existing_results, f, indent=2)
    except IOError as e:
        print(f"   Warning: Could not save to cache file {cache_file}: {e}")

def rename_existing_chart_files(output_folder: str = "rebalancing_backtest_results"):
    """Rename existing chart files to use the new number/equation format"""
    cache_file = os.path.join(output_folder, "rebalancing_backtest_cache.json")
    cached_results = load_cached_results(cache_file)
    
    if not cached_results:
        print(f"No cached results found. Cannot rename files.")
        return
    
    renamed_count = 0
    for result in cached_results:
        metric_key = result.get('metric', '').strip()
        if not metric_key:
            continue
        
        # Generate new filename
        new_filename_base = format_metric_name_for_filename(metric_key)
        
        # Try to find old files (could be single or combined format)
        old_patterns = [
            f"{metric_key.replace('/', '_').replace(' ', '_').lower()}_rebalancing_backtest.png",
            f"{metric_key.replace('/', '_').replace(' ', '_').lower()}_combined_rebalancing_backtest.png"
        ]
        
        # For combined metrics, also try the joined format
        if '+' in metric_key:
            metric_keys = [k.strip() for k in metric_key.split('+')]
            old_patterns.append('_'.join([m.replace('/', '_').replace(' ', '_').lower() for m in metric_keys]) + '_combined_rebalancing_backtest.png')
        
        new_filename = f"{new_filename_base}.png"
        new_path = os.path.join(output_folder, new_filename)
        
        # Skip if new file already exists
        if os.path.exists(new_path):
            continue
        
        # Try to rename old files
        for old_pattern in old_patterns:
            old_path = os.path.join(output_folder, old_pattern)
            if os.path.exists(old_path):
                try:
                    os.rename(old_path, new_path)
                    print(f"Renamed: {old_pattern} -> {new_filename}")
                    renamed_count += 1
                    break
                except Exception as e:
                    print(f"Error renaming {old_pattern}: {e}")
    
    if renamed_count > 0:
        print(f"\nRenamed {renamed_count} chart file(s)")
    else:
        print("No files needed renaming (either already renamed or not found)")

def format_metric_name_for_filename(metric_key: str) -> str:
    """Format metric key(s) for use in filenames
    - Single metrics: "(Number)MetricName" (e.g., "(1)Size")
    - Combinations: equation only (e.g., "2+3+6")
    """
    # Create mapping of metric keys to numbers (1-based)
    metric_key_to_number = {m.key: idx + 1 for idx, m in enumerate(METRICS)}
    
    metric_key = metric_key.strip()
    
    # Check if it's a combined metric (contains '+')
    if '+' in metric_key:
        # Parse the metric keys and convert to numbers
        metric_keys = [k.strip() for k in metric_key.split('+')]
        metric_numbers = []
        for key in metric_keys:
            if key and key in metric_key_to_number:
                metric_numbers.append(str(metric_key_to_number[key]))
        if metric_numbers:
            # Sort numbers for consistent display
            metric_numbers.sort(key=int)
            return '+'.join(metric_numbers)
        else:
            # Fallback if parsing fails
            return metric_key.replace('/', '_').replace(' ', '_').lower()
    else:
        # Single metric - return "(Number)MetricName"
        if metric_key and metric_key in metric_key_to_number:
            metric_config = METRICS_BY_KEY.get(metric_key)
            if metric_config:
                # Get metric display name (replace / and spaces with _ for filename safety)
                metric_name = metric_config.display_name.replace('/', '_').replace(' ', '_')
                metric_number = str(metric_key_to_number[metric_key])
                return f"({metric_number}){metric_name}"
            else:
                return str(metric_key_to_number[metric_key])
        else:
            # Fallback if key not found
            return metric_key.replace('/', '_').replace(' ', '_').lower()

def create_comparison_chart(results: List[Dict], output_folder: str, chart_type: str):
    """Create a comparison chart showing excess returns vs benchmark"""
    if not results:
        print(f"   No summary data to create comparison chart")
        return
    
    # Create mapping of metric keys to numbers (1-based)
    metric_key_to_number = {m.key: idx + 1 for idx, m in enumerate(METRICS)}
    
    # Sort by excess return (how much they beat the benchmark)
    results.sort(key=lambda x: x.get('excess_return', 0), reverse=True)
    
    # Format metric labels with numbers
    formatted_metrics = []
    for r in results:
        metric_name = r['metric_name']
        metric_key = r.get('metric', '').strip()
        
        # Check if it's a combined metric (contains '+')
        if '+' in metric_key:
            # Parse the metric keys and convert to numbers
            metric_keys = [k.strip() for k in metric_key.split('+')]
            metric_numbers = []
            for key in metric_keys:
                if key and key in metric_key_to_number:
                    metric_numbers.append(str(metric_key_to_number[key]))
            if metric_numbers:
                # Sort numbers for consistent display
                metric_numbers.sort(key=int)
                formatted_name = '+'.join(metric_numbers)
            else:
                formatted_name = metric_name  # Fallback if parsing fails
        else:
            # Single metric - add number in parentheses
            if metric_key and metric_key in metric_key_to_number:
                metric_number = metric_key_to_number[metric_key]
                formatted_name = f"{metric_name} ({metric_number})"
            else:
                formatted_name = metric_name  # Fallback if key not found
        
        formatted_metrics.append(formatted_name)
    
    metrics = formatted_metrics
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

# ============================================================================
# METRIC REGISTRY - ALL METRICS DEFINED HERE
# ============================================================================
# To add a new metric:
# 1. Create a getter function: get_my_metric_at_date(stock_data, target_year)
# 2. Add a MetricConfig entry below with:
#    - key: internal identifier (e.g., "my_metric")
#    - display_name: full name for display
#    - short_name: abbreviated name for charts
#    - getter_function: the function you created
#    - reverse_sort: True if lower values are better (e.g., EV/EBIT, consistency)
#    - quarters_of_history_needed: number of quarters of historical data needed (start year = 2002 + ceil(quarters/4))
#    - date_key: key for storing date in stock_entry (e.g., "my_metric_date")
#    - load_years: tuple of (start_year, end_year) for initial data loading
# 3. That's it! The metric will automatically be available everywhere.

def get_size_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get Size (Revenue) for a stock at a specific date - uses revenue as the metric value"""
    return get_revenue_at_date(stock_data, target_year)

METRICS: List[MetricConfig] = [
    MetricConfig(
        key="size",
        display_name="Size",
        short_name="Size",
        getter_function=get_size_at_date,
        reverse_sort=False,  # Higher revenue is better
        quarters_of_history_needed=0,
        date_key="size_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="ebit_ppe",
        display_name="EBIT/PPE",
        short_name="EBIT/PPE",
        getter_function=get_ebit_ppe_at_date,
        reverse_sort=False,
        quarters_of_history_needed=0,
        date_key="ebit_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="operating_margin",
        display_name="Operating Margin",
        short_name="Operating Margin",
        getter_function=get_operating_margin_at_date,
        reverse_sort=False,
        quarters_of_history_needed=0,
        date_key="om_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="gross_margin",
        display_name="Gross Margin",
        short_name="Gross Margin",
        getter_function=get_gross_margin_at_date,
        reverse_sort=False,
        quarters_of_history_needed=0,
        date_key="gm_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="5y_revenue_cagr",
        display_name="5-Year Revenue CAGR",
        short_name="5Y Rev CAGR",
        getter_function=get_5y_revenue_cagr_at_date,
        reverse_sort=False,
        quarters_of_history_needed=21,  # Needs 21 quarters (idx >= 20)
        date_key="cagr_date",
        load_years=(2007, 2009)
    ),
    MetricConfig(
        key="5y_revenue_growth_rate",
        display_name="5y halfway growth",
        short_name="5y halfway growth",
        getter_function=get_5y_revenue_growth_rate_at_date,
        reverse_sort=False,
        quarters_of_history_needed=20,  # Needs 20 quarters (idx >= 19)
        date_key="growth_rate_date",
        load_years=(2007, 2009)
    ),
    MetricConfig(
        key="consistency_of_growth",
        display_name="Consistency of Growth",
        short_name="Consistency Growth",
        getter_function=get_consistency_of_growth_at_date,
        reverse_sort=True,
        quarters_of_history_needed=20,  # Needs 20 quarters (idx >= 19)
        date_key="consistency_date",
        load_years=(2007, 2009)
    ),
    MetricConfig(
        key="consistency_of_operating_margin",
        display_name="Consistency of Operating Margin",
        short_name="Consistency OM",
        getter_function=get_consistency_of_operating_margin_at_date,
        reverse_sort=True,
        quarters_of_history_needed=20,  # Needs 20 quarters (idx >= 19)
        date_key="consistency_om_date",
        load_years=(2007, 2009)
    ),
    MetricConfig(
        key="dividend_yield",
        display_name="Dividend Yield",
        short_name="Dividend Yield",
        getter_function=get_dividend_yield_at_date,
        reverse_sort=False,
        quarters_of_history_needed=0,
        date_key="div_yield_date",
        load_years=(2001, 2005)  # Wider range for dividend yield
    ),
    MetricConfig(
        key="ev_to_ebit",
        display_name="EV/EBIT",
        short_name="EV/EBIT",
        getter_function=get_ev_to_ebit_at_date,
        reverse_sort=True,
        quarters_of_history_needed=0,
        date_key="ev_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="net_debt_to_ebit",
        display_name="Net Debt to EBIT",
        short_name="Net Debt/EBIT",
        getter_function=get_net_debt_to_ebit_at_date,
        reverse_sort=True,
        quarters_of_history_needed=0,
        date_key="net_debt_ebit_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="roa",
        display_name="ROA",
        short_name="ROA",
        getter_function=get_roa_at_date,
        reverse_sort=False,
        quarters_of_history_needed=0,
        date_key="roa_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="price_to_book",
        display_name="Price to Book",
        short_name="P/B",
        getter_function=get_price_to_book_at_date,
        reverse_sort=True,
        quarters_of_history_needed=0,
        date_key="pb_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="total_past_return",
        display_name="Total Past Return",
        short_name="Total Return",
        getter_function=get_total_past_return_at_date,
        reverse_sort=False,
        quarters_of_history_needed=0,
        date_key="total_return_date",
        load_years=(2002, 2004)
    ),
    MetricConfig(
        key="relative_ps",
        display_name="Relative PS",
        short_name="Relative PS",
        getter_function=get_relative_ps_at_date,
        reverse_sort=True,
        quarters_of_history_needed=20,  # Needs 20 quarters (idx >= 19)
        date_key="ps_date",
        load_years=(2007, 2009)
    ),
    MetricConfig(
        key="acceleration_of_growth",
        display_name="Acceleration of Growth",
        short_name="Acceleration Growth",
        getter_function=get_acceleration_of_growth_at_date,
        reverse_sort=False,
        quarters_of_history_needed=21,  # Needs 21 quarters (idx >= 20)
        date_key="acceleration_date",
        load_years=(2008, 2010)
    ),
    MetricConfig(
        key="5y_share_growth",
        display_name="5y share growth",
        short_name="5y share growth",
        getter_function=get_5y_share_growth_at_date,
        reverse_sort=True,  # Lower is better (fewer shares is better)
        quarters_of_history_needed=20,  # Needs 20 quarters (idx >= 19)
        date_key="share_growth_date",
        load_years=(2007, 2009)
    ),
    MetricConfig(
        key="ttm_share_buyback_to_market_cap",
        display_name="TTM Share Buyback to Market Cap",
        short_name="TTM Buyback/MCap",
        getter_function=get_ttm_share_buyback_to_market_cap_at_date,
        reverse_sort=False,  # Higher is better (more buybacks is better)
        quarters_of_history_needed=4,  # Needs 4 quarters (idx >= 4)
        date_key="buyback_mcap_date",
        load_years=(2003, 2005)
    ),
]

# Create lookup dictionaries for efficient access
METRICS_BY_KEY = {m.key: m for m in METRICS}
METRICS_GETTER_FUNCTIONS = {m.key: m.getter_function for m in METRICS}
REVERSE_SORT_METRICS = {m.key: m.reverse_sort for m in METRICS if m.reverse_sort}
METRICS_QUARTERS_OF_HISTORY = {m.key: m.quarters_of_history_needed for m in METRICS}
METRICS_YEARS_OF_HISTORY = {m.key: m.years_of_history_needed for m in METRICS}  # For backward compatibility
METRICS_DATE_KEYS = {m.key: m.date_key for m in METRICS}

# Backward compatibility - metrics needing 5 years of history
METRICS_NEEDING_5Y_HISTORY = {m.key for m in METRICS if m.years_of_history_needed >= 5}

def get_start_year_for_metric(metric_key: str) -> int:
    """Get the start year for a metric: 2002 + ceil(quarters_of_history_needed / 4)"""
    quarters_needed = METRICS_QUARTERS_OF_HISTORY.get(metric_key, 0)
    years_needed = math.ceil(quarters_needed / 4.0)
    return 2002 + years_needed

def get_start_year_for_combined_metrics(metric_keys: List[str]) -> int:
    """Get the start year for combined metrics: 2002 + max(ceil(quarters_of_history_needed / 4))"""
    max_quarters_needed = max([METRICS_QUARTERS_OF_HISTORY.get(key, 0) for key in metric_keys], default=0)
    max_years_needed = math.ceil(max_quarters_needed / 4.0)
    return 2002 + max_years_needed

def get_metric_at_date(stock_data: Dict, metric_name: str, target_year: int) -> Optional[Tuple[float, str]]:
    """Get a metric value for a stock at a specific year"""
    getter_func = METRICS_GETTER_FUNCTIONS.get(metric_name)
    if getter_func:
        return getter_func(stock_data, target_year)
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
    # Use registry to determine if this is a reverse sort metric (lower is better)
    metric_config = METRICS_BY_KEY.get(metric_name)
    reverse_sort = not metric_config.reverse_sort if metric_config else True  # Default to higher is better
    
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

def calculate_weights_for_period_combined(stocks: List[Dict], metric_names: List[str], initial_revenue_total: float) -> Dict[str, float]:
    """
    Calculate metric-weighted portfolio weights for a period using combined metrics
    Combines rankings from multiple metrics by summing normalized ranks
    Uses initial revenue for base weights, but new combined metric rankings
    Returns dict: ticker -> final_weight_pct
    """
    # Get all stocks that have at least one metric
    stocks_with_any_metric = []
    for stock in stocks:
        has_any_metric = False
        for metric_name in metric_names:
            metric_key = f'current_metric_value_{metric_name}'
            if stock.get(metric_key) is not None:
                has_any_metric = True
                break
        if has_any_metric:
            stocks_with_any_metric.append(stock)
    
    if not stocks_with_any_metric:
        return {}
    
    # For each metric, calculate normalized ranks (0 = best, 1 = worst)
    # Calculate ranks for each metric
    for metric_name in metric_names:
        metric_key = f'current_metric_value_{metric_name}'
        stocks_with_this_metric = [s for s in stocks_with_any_metric if s.get(metric_key) is not None]
        
        if not stocks_with_this_metric:
            continue
        
        # Sort by metric value
        # Use registry to determine if this is a reverse sort metric (lower is better)
        metric_config = METRICS_BY_KEY.get(metric_name)
        reverse_sort = not metric_config.reverse_sort if metric_config else True  # Default to higher is better
        
        if reverse_sort:
            stocks_with_this_metric.sort(key=lambda x: x[metric_key], reverse=True)
        else:
            stocks_with_this_metric.sort(key=lambda x: x[metric_key] if x[metric_key] is not None else float('inf'))
        
        # Assign normalized ranks (0 = best, 1 = worst)
        n_with_metric = len(stocks_with_this_metric)
        for i, stock in enumerate(stocks_with_this_metric):
            rank_key = f'normalized_rank_{metric_name}'
            if n_with_metric > 1:
                stock[rank_key] = i / (n_with_metric - 1)  # 0 to 1
            else:
                stock[rank_key] = 0.0
    
    # Calculate combined rank (sum of normalized ranks, lower is better)
    for stock in stocks_with_any_metric:
        combined_rank = 0.0
        metric_count = 0
        for metric_name in metric_names:
            rank_key = f'normalized_rank_{metric_name}'
            if rank_key in stock:
                combined_rank += stock[rank_key]
                metric_count += 1
        
        if metric_count > 0:
            stock['combined_rank'] = combined_rank / metric_count  # Average normalized rank
        else:
            stock['combined_rank'] = float('inf')  # No metrics available
    
    # Filter to stocks with valid combined rank
    stocks_with_combined_rank = [s for s in stocks_with_any_metric if s.get('combined_rank') != float('inf')]
    
    if not stocks_with_combined_rank:
        return {}
    
    # Sort by combined rank (lower is better)
    stocks_with_combined_rank.sort(key=lambda x: x['combined_rank'])
    
    # Calculate initial weights based on initial revenue
    for stock in stocks_with_combined_rank:
        stock['initial_weight'] = (stock['initial_revenue'] / initial_revenue_total) * 100
    
    # Apply multiplier based on combined rank (0.5 for worst, 2.0 for best)
    n_stocks = len(stocks_with_combined_rank)
    for i, stock in enumerate(stocks_with_combined_rank):
        if n_stocks > 1:
            multiplier = 2.0 - (i / (n_stocks - 1)) * 1.5  # 2.0 to 0.5
        else:
            multiplier = 1.0
        stock['multiplier'] = multiplier
        stock['adjusted_weight'] = stock['initial_weight'] * multiplier
    
    # Normalize weights to sum to 100%
    total_adjusted = sum(s['adjusted_weight'] for s in stocks_with_combined_rank)
    weights = {}
    for stock in stocks_with_combined_rank:
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
    
    # Determine start year based on metric requirements: 2002 + years_of_history_needed
    start_year = get_start_year_for_metric(selected_metric)
    
    # Get date key from registry
    metric_date_key = METRICS_DATE_KEYS.get(selected_metric, 'revenue_date')
    
    # Filter stocks that have the selected metric at the start year
    # For 5y metrics, we need to verify the metric is available at 2007
    # For dividend yield, check a wider range (2001-2004) to find more stocks
    # We'll check all stocks from the base list, not just those that already have the metric
    stock_info = []
    check_years = [start_year]
    if selected_metric == 'dividend_yield':
        # Check a wider range for dividend yield to find more stocks
        check_years = list(range(max(2001, start_year - 1), min(2005, start_year + 3)))
    
    for s in stock_info_base:
        metric_result = None
        metric_year_used = None
        
        # Check if metric is available in the year range
        for check_year in check_years:
            metric_result = get_metric_at_date(s['stock_data'], selected_metric, check_year)
            if metric_result:
                metric_year_used = check_year
                break
        
        if metric_result:
            stock_copy = s.copy()
            stock_copy['ticker'] = stock_copy.get('ticker') or stock_copy.get('symbol')
            
            # Get revenue from the year where we found the metric (or start_year)
            revenue_year = metric_year_used if metric_year_used else start_year
            revenue_result = get_revenue_at_date(s['stock_data'], revenue_year)
            if revenue_result:
                revenue, revenue_date = revenue_result
                stock_copy['initial_revenue'] = revenue
                stock_copy['initial_revenue_date'] = revenue_date
            else:
                # Fallback to existing revenue if not available
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
    
    # First, find a common start date for all stocks
    # Use the earliest date where we have data for all stocks
    all_stock_start_dates = []
    for stock in stock_info:
        start_date = stock.get(metric_date_key) or stock.get('initial_revenue_date')
        if start_date:
            all_stock_start_dates.append(start_date)
    
    # Use the earliest start date as the common portfolio start date
    # This ensures all stocks start calculating returns from the same point
    portfolio_start_date = None
    if all_stock_start_dates:
        portfolio_start_date = min(all_stock_start_dates)
        print(f"   Using common portfolio start date: {portfolio_start_date}")
    
    # Calculate returns for all stocks from the common start date
    all_dates = set()
    stock_returns_by_date = {}  # ticker -> {date: return_pct}
    
    for stock in stock_info:
        # Use the common portfolio start date for all stocks
        returns = calculate_total_return_with_dividends(stock['stock_data'], start_year, portfolio_start_date)
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
    # Initialize with first rebalancing weights
    current_metric_weights = {}
    
    # Process each date
    last_rebalance_year = None
    portfolio_value_metric = 1.0  # Start at 1.0
    portfolio_value_revenue = 1.0  # Start at 1.0
    
    # Track previous cumulative returns per stock to calculate period returns
    stock_prev_cumulative_return = {ticker: 0.0 for ticker in revenue_weights.keys()}
    
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
                new_weights = calculate_weights_for_period(
                    stocks_with_metric, selected_metric, initial_revenue_total
                )
                
                # For stocks that lost their metric, redistribute their weight proportionally
                # This prevents portfolio collapse when stocks temporarily lose metric data
                if current_metric_weights and len(new_weights) < len(current_metric_weights):
                    # Some stocks lost their metric - redistribute their weight
                    total_new_weight = sum(new_weights.values())
                    if total_new_weight > 0 and total_new_weight < 100.0:
                        # Redistribute the missing weight proportionally
                        weight_redistribution_factor = 100.0 / total_new_weight
                        for ticker in new_weights:
                            new_weights[ticker] *= weight_redistribution_factor
                    elif total_new_weight == 0:
                        # All stocks lost metric - keep previous weights
                        print(f"      Warning: All stocks lost metric data at {current_year}, keeping previous weights")
                        new_weights = current_metric_weights.copy()
                
                current_metric_weights = new_weights
                print(f"      Rebalanced {len(stocks_with_metric)} stocks with metric data")
            else:
                # No stocks with metric - if we have previous weights, keep them; otherwise this is an error
                if not current_metric_weights:
                    print(f"      Error: No stocks with metric data at {current_year} and no previous weights")
                else:
                    print(f"      Warning: No stocks with metric data at {current_year}, keeping previous weights")
            
            last_rebalance_year = current_year
        
        # Calculate portfolio period return for this date
        # Convert cumulative returns to period returns and compound
        period_return_metric = 0.0
        period_return_revenue = 0.0
        total_weight_metric = 0.0
        total_weight_revenue = 0.0
        
        for stock in stock_info:
            ticker = stock['ticker']
            metric_weight = current_metric_weights.get(ticker, 0.0) / 100.0
            revenue_weight = revenue_weights.get(ticker, 0.0) / 100.0
            
            # Get cumulative return for this date
            stock_cumulative_return_pct = None
            if ticker in stock_returns_by_date and date in stock_returns_by_date[ticker]:
                stock_cumulative_return_pct = stock_returns_by_date[ticker][date]
            
            # Use last known return if current date doesn't have data
            if stock_cumulative_return_pct is None:
                stock_cumulative_return_pct = stock_prev_cumulative_return.get(ticker, 0.0)
            
            # Calculate period return: (current_cumulative - previous_cumulative) / (1 + previous_cumulative/100)
            prev_cumulative = stock_prev_cumulative_return.get(ticker, 0.0)
            if prev_cumulative != stock_cumulative_return_pct:
                # Period return = (new_cumulative - old_cumulative) / (1 + old_cumulative/100)
                period_return_pct = (stock_cumulative_return_pct - prev_cumulative) / (1.0 + prev_cumulative / 100.0)
            else:
                period_return_pct = 0.0
            
            # Update previous cumulative return
            stock_prev_cumulative_return[ticker] = stock_cumulative_return_pct
            
            # Weighted period return contribution
            period_return_metric += metric_weight * period_return_pct
            period_return_revenue += revenue_weight * period_return_pct
            total_weight_metric += metric_weight
            total_weight_revenue += revenue_weight
        
        # Normalize by total weight (in case weights don't sum to 1.0)
        if total_weight_metric > 0:
            period_return_metric /= total_weight_metric
        if total_weight_revenue > 0:
            period_return_revenue /= total_weight_revenue
        
        # Compound portfolio values
        portfolio_value_metric *= (1.0 + period_return_metric / 100.0)
        portfolio_value_revenue *= (1.0 + period_return_revenue / 100.0)
        
        # Calculate cumulative returns from portfolio values
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
    
    metric_filename = format_metric_name_for_filename(selected_metric)
    chart_filename = f'{output_folder}/{metric_filename}.png'
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"      Chart saved to {chart_filename}")
    plt.close()
    
    # Return summary data for comparison chart (don't save to file)
    summary_data = {
        'metric': selected_metric,
        'metric_name': metric_name,
        'start_year': start_year,
        'start_date': sorted_dates[0].strftime('%Y-%m-%d'),
        'end_date': sorted_dates[-1].strftime('%Y-%m-%d'),
        'metric_weighted_annualized': annualized_return_metric,
        'revenue_weighted_annualized': annualized_return_revenue,
        'excess_return': annualized_return_metric - annualized_return_revenue
    }
    
    print(f"\n   Completed rebalancing backtest for {metric_name}")
    return summary_data

def run_rebalancing_backtest_for_combined_metrics(stock_info_base: List[Dict], selected_metrics: List[str], metric_names: List[str], combined_metric_name: str):
    """Run rebalancing backtest for combined metrics"""
    print("\n" + "=" * 80)
    print(f"Running REBALANCING backtest for COMBINED METRICS: {combined_metric_name}")
    print("=" * 80)
    
    # Determine start year based on metric requirements: 2002 + max(years_of_history_needed)
    start_year = get_start_year_for_combined_metrics(selected_metrics)
    
    # Filter stocks that have at least one of the selected metrics at the start year
    stock_info = []
    check_years = [start_year]
    if 'dividend_yield' in selected_metrics:
        check_years = list(range(max(2001, start_year - 1), min(2005, start_year + 3)))
    
    for s in stock_info_base:
        has_at_least_one_metric = False
        metric_year_used = None
        
        # Check if at least one metric is available in the year range
        for check_year in check_years:
            for selected_metric in selected_metrics:
                metric_result = get_metric_at_date(s['stock_data'], selected_metric, check_year)
                if metric_result:
                    has_at_least_one_metric = True
                    metric_year_used = check_year
                    break
            if has_at_least_one_metric:
                break
        
        if has_at_least_one_metric:
            stock_copy = s.copy()
            stock_copy['ticker'] = stock_copy.get('ticker') or stock_copy.get('symbol')
            
            # Get revenue from the year where we found the metric (or start_year)
            revenue_year = metric_year_used if metric_year_used else start_year
            revenue_result = get_revenue_at_date(s['stock_data'], revenue_year)
            if revenue_result:
                revenue, revenue_date = revenue_result
                stock_copy['initial_revenue'] = revenue
                stock_copy['initial_revenue_date'] = revenue_date
            else:
                stock_copy['initial_revenue'] = s['revenue']
                stock_copy['initial_revenue_date'] = s.get('revenue_date')
            
            stock_info.append(stock_copy)
    
    if not stock_info:
        print(f"   Skipping {combined_metric_name}: No stocks found with metric data for {start_year}")
        return
    
    # Sort by initial revenue and take top 500
    stock_info.sort(key=lambda x: x['initial_revenue'], reverse=True)
    stock_info = stock_info[:500]
    
    initial_revenue_total = sum(s['initial_revenue'] for s in stock_info)
    print(f"   Using {len(stock_info)} stocks with initial revenue total: ${initial_revenue_total/1e9:.2f}B")
    
    # Get all available dates from stock returns
    print(f"\n   Calculating returns and collecting dates...")
    
    # First, find a common start date for all stocks
    # Use the earliest date where we have data for all stocks
    all_stock_start_dates = []
    for stock in stock_info:
        start_date = stock.get('initial_revenue_date')
        if start_date:
            all_stock_start_dates.append(start_date)
    
    # Use the earliest start date as the common portfolio start date
    # This ensures all stocks start calculating returns from the same point
    portfolio_start_date = None
    if all_stock_start_dates:
        portfolio_start_date = min(all_stock_start_dates)
        print(f"   Using common portfolio start date: {portfolio_start_date}")
    
    # Calculate returns for all stocks from the common start date
    all_dates = set()
    stock_returns_by_date = {}
    
    for stock in stock_info:
        # Use the common portfolio start date for all stocks
        returns = calculate_total_return_with_dividends(stock['stock_data'], start_year, portfolio_start_date)
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
    
    dates_by_year = defaultdict(list)
    for date in sorted_dates:
        dates_by_year[date.year].append(date)
    
    rebalance_years = sorted(dates_by_year.keys())
    print(f"   Will rebalance at years: {rebalance_years[:5]}...{rebalance_years[-5:] if len(rebalance_years) > 10 else rebalance_years}")
    
    # Calculate revenue-weighted portfolio weights (static)
    revenue_weights = {}
    for stock in stock_info:
        ticker = stock['ticker']
        revenue_weights[ticker] = (stock['initial_revenue'] / initial_revenue_total) * 100
    
    cumulative_returns_metric_weighted = []
    cumulative_returns_revenue_weighted = []
    current_metric_weights = {}
    
    last_rebalance_year = None
    portfolio_value_metric = 1.0
    portfolio_value_revenue = 1.0
    
    # Track previous cumulative returns per stock to calculate period returns
    stock_prev_cumulative_return = {ticker: 0.0 for ticker in revenue_weights.keys()}
    
    for date in sorted_dates:
        current_year = date.year
        
        # Rebalance if we've entered a new year
        if last_rebalance_year is None or current_year > last_rebalance_year:
            print(f"   Rebalancing at {current_year}...")
            
            # Recalculate all metrics for all stocks at this year
            for stock in stock_info:
                for selected_metric in selected_metrics:
                    metric_result = get_metric_at_date(stock['stock_data'], selected_metric, current_year)
                    metric_key = f'current_metric_value_{selected_metric}'
                    if metric_result:
                        metric_value, metric_date = metric_result
                        stock[metric_key] = metric_value
                    else:
                        stock[metric_key] = None
            
            # Calculate new weights based on combined metric rankings
            new_weights = calculate_weights_for_period_combined(
                stock_info, selected_metrics, initial_revenue_total
            )
            
            # Handle weight redistribution if needed
            if current_metric_weights and len(new_weights) < len(current_metric_weights):
                total_new_weight = sum(new_weights.values())
                if total_new_weight > 0 and total_new_weight < 100.0:
                    weight_redistribution_factor = 100.0 / total_new_weight
                    for ticker in new_weights:
                        new_weights[ticker] *= weight_redistribution_factor
                elif total_new_weight == 0:
                    print(f"      Warning: All stocks lost metric data at {current_year}, keeping previous weights")
                    new_weights = current_metric_weights.copy()
            
            current_metric_weights = new_weights
            print(f"      Rebalanced {len([s for s in stock_info if any(s.get(f'current_metric_value_{m}') is not None for m in selected_metrics)])} stocks with metric data")
            
            last_rebalance_year = current_year
        
        # Calculate portfolio period return for this date
        # Convert cumulative returns to period returns and compound
        period_return_metric = 0.0
        period_return_revenue = 0.0
        total_weight_metric = 0.0
        total_weight_revenue = 0.0
        
        for stock in stock_info:
            ticker = stock['ticker']
            metric_weight = current_metric_weights.get(ticker, 0.0) / 100.0
            revenue_weight = revenue_weights.get(ticker, 0.0) / 100.0
            
            # Get cumulative return for this date
            stock_cumulative_return_pct = None
            if ticker in stock_returns_by_date and date in stock_returns_by_date[ticker]:
                stock_cumulative_return_pct = stock_returns_by_date[ticker][date]
            
            # Use last known return if current date doesn't have data
            if stock_cumulative_return_pct is None:
                stock_cumulative_return_pct = stock_prev_cumulative_return.get(ticker, 0.0)
            
            # Calculate period return: (current_cumulative - previous_cumulative) / (1 + previous_cumulative/100)
            prev_cumulative = stock_prev_cumulative_return.get(ticker, 0.0)
            if prev_cumulative != stock_cumulative_return_pct:
                # Period return = (new_cumulative - old_cumulative) / (1 + old_cumulative/100)
                period_return_pct = (stock_cumulative_return_pct - prev_cumulative) / (1.0 + prev_cumulative / 100.0)
            else:
                period_return_pct = 0.0
            
            # Update previous cumulative return
            stock_prev_cumulative_return[ticker] = stock_cumulative_return_pct
            
            # Weighted period return contribution
            period_return_metric += metric_weight * period_return_pct
            period_return_revenue += revenue_weight * period_return_pct
            total_weight_metric += metric_weight
            total_weight_revenue += revenue_weight
        
        # Normalize by total weight (in case weights don't sum to 1.0)
        if total_weight_metric > 0:
            period_return_metric /= total_weight_metric
        if total_weight_revenue > 0:
            period_return_revenue /= total_weight_revenue
        
        # Compound portfolio values
        portfolio_value_metric *= (1.0 + period_return_metric / 100.0)
        portfolio_value_revenue *= (1.0 + period_return_revenue / 100.0)
        
        # Calculate cumulative returns from portfolio values
        cumulative_return_metric = (portfolio_value_metric - 1.0) * 100
        cumulative_return_revenue = (portfolio_value_revenue - 1.0) * 100
        cumulative_returns_metric_weighted.append(cumulative_return_metric)
        cumulative_returns_revenue_weighted.append(cumulative_return_revenue)
    
    print(f"\n   Calculated returns for {len(sorted_dates)} time periods")
    print(f"      Start date: {sorted_dates[0].strftime('%Y-%m-%d')}")
    print(f"      End date: {sorted_dates[-1].strftime('%Y-%m-%d')}")
    print(f"      {combined_metric_name} Weighted (Rebalanced) Total return: {cumulative_returns_metric_weighted[-1]:.2f}%")
    print(f"      Revenue Weighted (Static) Total return: {cumulative_returns_revenue_weighted[-1]:.2f}%")
    
    start_date_obj = sorted_dates[0]
    end_date_obj = sorted_dates[-1]
    delta_years = (end_date_obj - start_date_obj).days / 365.25
    
    annualized_return_metric = ((1 + cumulative_returns_metric_weighted[-1] / 100) ** (1 / delta_years) - 1) * 100 if delta_years > 0 else 0.0
    annualized_return_revenue = ((1 + cumulative_returns_revenue_weighted[-1] / 100) ** (1 / delta_years) - 1) * 100 if delta_years > 0 else 0.0
    
    print(f"      {combined_metric_name} Weighted (Rebalanced) Annualized return: {annualized_return_metric:.2f}%")
    print(f"      Revenue Weighted (Static) Annualized return: {annualized_return_revenue:.2f}%")
    
    output_folder = "rebalancing_backtest_results"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    print(f"\n   Creating performance chart...")
    plt.figure(figsize=(14, 8))
    
    plt.plot(sorted_dates, cumulative_returns_metric_weighted, linewidth=2, 
             color='#2E86AB', label=f'{combined_metric_name} Weighted (Rebalanced) ({annualized_return_metric:+.1f}% ann.)')
    plt.plot(sorted_dates, cumulative_returns_revenue_weighted, linewidth=2, 
             color='#A23B72', label=f'Revenue Weighted (Static) ({annualized_return_revenue:+.1f}% ann.)', linestyle='--')
    
    plt.title(f'Rebalancing Portfolio Performance Comparison ({start_year} - Present)\n'
              f'{combined_metric_name} Weighted (Annual Rebalancing) vs Revenue Weighted (Static)',
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
    
    # Format combined metrics as equation (e.g., "1+3")
    combined_metric_key = '+'.join(selected_metrics)
    metric_filename = format_metric_name_for_filename(combined_metric_key)
    chart_filename = f'{output_folder}/{metric_filename}.png'
    plt.savefig(chart_filename, dpi=300, bbox_inches='tight')
    print(f"      Chart saved to {chart_filename}")
    plt.close()
    
    summary_data = {
        'metric': '+'.join(selected_metrics),
        'metric_name': combined_metric_name,
        'start_year': start_year,
        'start_date': sorted_dates[0].strftime('%Y-%m-%d'),
        'end_date': sorted_dates[-1].strftime('%Y-%m-%d'),
        'metric_weighted_annualized': annualized_return_metric,
        'revenue_weighted_annualized': annualized_return_revenue,
        'excess_return': annualized_return_metric - annualized_return_revenue
    }
    
    print(f"\n   Completed rebalancing backtest for {combined_metric_name}")
    return summary_data

def main():
    """Main function"""
    print("=" * 80)
    print("REBALANCING Portfolio Backtest (Annual Rebalancing)")
    print("=" * 80)
    
    # Define all metrics
    # Generate all_metrics from METRICS registry
    all_metrics = [
        {
            "selected_metric": m.key,
            "metric_name": m.display_name,
            "metric_display_name": m.short_name
        }
        for m in METRICS
    ]
    
    # User input for metric selection
    print("\nAvailable metrics:")
    for i, metric in enumerate(all_metrics, 1):
        print(f"  {i}. {metric['metric_name']}")
    print(f"  {len(all_metrics) + 1}. Run all metrics")
    print(f"\n  You can also combine metrics using + (e.g., '1+3' for metrics 1 and 3, or '1+3+6' for three metrics)")
    
    while True:
        try:
            user_input = input(f"\nSelect metric (1-{len(all_metrics) + 1}, or combine with + like '1+3' or '1+3+6'): ").strip()
            
            # Check if input contains + (combined metrics)
            if '+' in user_input:
                # Parse combined metrics
                parts = user_input.split('+')
                metric_indices = []
                for part in parts:
                    part = part.strip()
                    if part.isdigit():
                        idx = int(part)
                        if 1 <= idx <= len(all_metrics):
                            metric_indices.append(idx - 1)  # Convert to 0-based
                        else:
                            print(f"Invalid metric number: {idx}. Please enter numbers between 1 and {len(all_metrics)}.")
                            break
                    else:
                        print(f"Invalid input: '{part}'. Please enter numbers separated by +.")
                        break
                else:
                    # All parts were valid
                    if len(metric_indices) < 2:
                        print("Please combine at least 2 metrics (e.g., '1+3')")
                    else:
                        # Create combined metric entry
                        selected_metrics = [all_metrics[i]['selected_metric'] for i in metric_indices]
                        metric_names = [all_metrics[i]['metric_name'] for i in metric_indices]
                        combined_name = " + ".join(metric_names)
                        metrics_to_run = [{
                            'selected_metrics': selected_metrics,
                            'metric_names': metric_names,
                            'metric_name': combined_name,
                            'metric_display_name': combined_name,
                            'is_combined': True
                        }]
                        print(f"\nSelected combined metrics: {combined_name}")
                        break
            else:
                # Single metric or "all"
                choice = int(user_input)
                
                if choice == len(all_metrics) + 1:
                    # Run all metrics
                    metrics_to_run = all_metrics
                    break
                elif 1 <= choice <= len(all_metrics):
                    # Run single metric
                    metrics_to_run = [all_metrics[choice - 1]]
                    break
                else:
                    print(f"Invalid choice. Please enter a number between 1 and {len(all_metrics) + 1}.")
        except ValueError:
            print("Invalid input. Please enter a number or combine metrics with + (e.g., '1+3').")
        except KeyboardInterrupt:
            print("\n\nExiting...")
            return
    
    # Start timer only after user has made their selection
    start_time = time.time()
    
    if not any(m.get('is_combined', False) for m in metrics_to_run):
        print(f"\nSelected {len(metrics_to_run)} metric(s) to run:")
        for metric in metrics_to_run:
            print(f"  - {metric['metric_name']}")
    
    print("\n1. Finding S&P 500-like stocks from 2002...")
    print("   (Using stocks with data from 2002, ranked by revenue)")
    
    # Start data loading timer
    data_loading_start = time.time()
    
    # Load all stock data
    print("   Loading all stock data...")
    nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
    nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
    all_stocks_list = nyse_stocks + nasdaq_stocks
    
    print(f"   Loaded {len(all_stocks_list)} stocks")
    
    # Find stocks with data around 2002
    print("   Finding stocks with data from 2002 (2007 for metrics requiring 5 years of history)...")
    stocks_with_data = []
    
    # Group metrics by load_years for efficient loading
    metrics_by_load_years = {}
    for metric in METRICS:
        load_years_key = metric.load_years
        if load_years_key not in metrics_by_load_years:
            metrics_by_load_years[load_years_key] = []
        metrics_by_load_years[load_years_key].append(metric)
    
    for stock_data in all_stocks_list:
        # Load revenue first and check threshold early - skip metric calculation if revenue too low
        revenue_result = None
        for year in range(2002, 2004):
            revenue_result = get_revenue_at_date(stock_data, year)
            if revenue_result:
                break
        
        # Early exit if revenue is too low - saves calculating all metrics for stocks that will be filtered
        if not revenue_result or revenue_result[0] < 100_000_000:
            continue
        
        # Only calculate metrics for stocks that pass revenue threshold
        revenue, revenue_date = revenue_result
        metric_results = {}
        
        # Load all metrics grouped by their load_years
        # Restructured to break early once each metric is found
        for load_years, metrics_group in metrics_by_load_years.items():
            start_year, end_year = load_years
            for metric in metrics_group:
                metric_key = metric.key
                # Skip if already found
                if metric_key in metric_results and metric_results[metric_key]:
                    continue
                
                # Try years until we find the metric, then break
                for year in range(start_year, end_year):
                    result = metric.getter_function(stock_data, year)
                    if result:
                        metric_results[metric_key] = result
                        break  # Found it, move to next metric
        
        # Create stock entry
        stock_entry = {
            'ticker': stock_data.get('symbol'),
            'stock_data': stock_data,
            'revenue': revenue,
            'revenue_date': revenue_date
        }
        
        # Store all metric results dynamically from registry
        for metric in METRICS:
            metric_key = metric.key
            if metric_key in metric_results and metric_results[metric_key]:
                metric_value, metric_date = metric_results[metric_key]
                stock_entry[metric_key] = metric_value
                stock_entry[metric.date_key] = metric_date
        
        stocks_with_data.append(stock_entry)
    
    print(f"   Found {len(stocks_with_data)} stocks with data and revenue >= $100M")
    
    # Sort by revenue for reference, but don't limit yet - let each metric function select appropriately
    stocks_with_data.sort(key=lambda x: x['revenue'], reverse=True)
    
    print(f"   Found {len(stocks_with_data)} stocks with data and revenue >= $100M")
    
    # End data loading timer
    data_loading_end = time.time()
    data_loading_time = data_loading_end - data_loading_start
    
    # Create output folder
    output_folder = "rebalancing_backtest_results"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"\n   Created output folder: {output_folder}/")
    else:
        print(f"\n   Using output folder: {output_folder}/")
    
    # Load cached results
    cache_file = os.path.join(output_folder, "rebalancing_backtest_cache.json")
    cached_results = load_cached_results(cache_file)
    print(f"   Loaded {len(cached_results)} cached result(s)")
    
    # Run rebalancing backtest for selected metrics
    print("\n2. Running rebalancing backtests...")
    
    # Start backtest calculations timer
    backtest_start = time.time()
    
    new_summary_results = []
    for metric in metrics_to_run:
        if metric.get('is_combined', False):
            # Run combined metrics backtest
            summary_data = run_rebalancing_backtest_for_combined_metrics(
                stocks_with_data, 
                metric['selected_metrics'], 
                metric['metric_names'], 
                metric['metric_name']
            )
        else:
            # Run single metric backtest
            summary_data = run_rebalancing_backtest_for_metric(
                stocks_with_data, 
                metric['selected_metric'], 
                metric['metric_name'], 
                metric['metric_display_name']
            )
        if summary_data:
            new_summary_results.append(summary_data)
            # Save to cache immediately
            save_result_to_cache(cache_file, summary_data)
            print(f"      Saved result to cache")
    
    # End backtest calculations timer
    backtest_end = time.time()
    backtest_time = backtest_end - backtest_start
    
    # Merge cached results with new results
    # Create a dict keyed by 'metric' to avoid duplicates
    all_results_dict = {}
    
    # Add cached results first
    for result in cached_results:
        metric_key = result.get('metric')
        if metric_key:
            all_results_dict[metric_key] = result
    
    # Overwrite with new results (newer results take precedence)
    for result in new_summary_results:
        metric_key = result.get('metric')
        if metric_key:
            all_results_dict[metric_key] = result
    
    # Convert back to list
    all_results = list(all_results_dict.values())
    
    # Always create/update comparison chart with all results
    if all_results:
        print(f"\n3. Renaming existing chart files to new format...")
        rename_existing_chart_files(output_folder)
        print(f"\n   Creating metric comparison chart with {len(all_results)} metric(s)...")
        create_comparison_chart(all_results, output_folder, "Rebalancing")
    else:
        print("\n3. No results available for comparison chart")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("\n" + "=" * 80)
    print("All Rebalancing Backtests Complete!")
    print("=" * 80)
    
    # Format and display timing statistics
    def format_time(seconds):
        """Format time in seconds to human-readable format"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s ({seconds:.2f} seconds)"
        elif minutes > 0:
            return f"{minutes}m {secs}s ({seconds:.2f} seconds)"
        else:
            return f"{secs}s ({seconds:.2f} seconds)"
    
    print(f"\nTiming Statistics:")
    print(f"  Data Loading: {format_time(data_loading_time)}")
    print(f"  Backtest Calculations: {format_time(backtest_time)}")
    print(f"  Total Execution Time: {format_time(elapsed_time)}")
    print("=" * 80)

if __name__ == "__main__":
    main()

