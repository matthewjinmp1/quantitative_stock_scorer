"""
Program to calculate stock metrics and percentiles for stocks from both NYSE and NASDAQ data files.
Uses a metric-based architecture that makes it easy to add new metrics.

Usage:
    python scorer.py calc              - Calculate and save scores for all stocks
    python scorer.py <symbol>         - Look up percentile rank for a specific stock (e.g., AAPL)
    python scorer.py view [N]         - View all stocks ranked by percentile (optionally show top N)
    python scorer.py view [N] over [X] - View top N stocks with market cap over X billion (e.g., view 50 over 10)
"""
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

# ============================================================================
# METRIC DEFINITIONS
# ============================================================================
# To add a new metric, simply add a new MetricConfig to the METRICS list below

@dataclass
class MetricConfig:
    """Configuration for a single metric"""
    key: str  # Internal key name (e.g., "ebit_ppe")
    display_name: str  # Display name (e.g., "EBIT/PPE")
    description: str  # Description for metadata
    calculator: Callable[[Dict], Optional[Tuple[str, str, float, str]]]  # Function that calculates the metric
    sort_descending: bool = True  # True if higher values are better, False if lower values are better
    include_in_total: bool = True  # Whether to include in total percentile calculation

# Helper function to extract period dates from stock data
def _get_period_dates(data: Dict) -> Optional[List]:
    """Extract period dates from data dictionary"""
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            return data[date_key]
    return None

# Metric calculation functions
def _calc_ebit_ppe(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate EBIT/PPE = Operating Income / PPE"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    ppe_net = data.get("ppe_net", [])
    
    if not isinstance(operating_income, list) or not isinstance(ppe_net, list):
        return None
    
    for j in range(len(period_dates) - 1, -1, -1):
        if j < len(operating_income) and j < len(ppe_net):
            if operating_income[j] is not None and ppe_net[j] is not None and ppe_net[j] != 0:
                ebit_ppe = operating_income[j] / ppe_net[j]
                return (symbol, company_name, ebit_ppe, period_dates[j])
    return None

def _calc_gross_margin(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Gross Margin = (Revenue - Cost of Goods Sold) / Revenue"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    cost_of_goods_sold = data.get("cost_of_goods_sold", [])
    # Also try alternative key name
    if not cost_of_goods_sold:
        cost_of_goods_sold = data.get("cogs", [])
    
    if not isinstance(revenue, list) or not isinstance(cost_of_goods_sold, list):
        return None
    
    for j in range(len(period_dates) - 1, -1, -1):
        if j < len(revenue) and j < len(cost_of_goods_sold):
            if revenue[j] is not None and cost_of_goods_sold[j] is not None and revenue[j] != 0:
                gross_margin = (revenue[j] - cost_of_goods_sold[j]) / revenue[j]
                return (symbol, company_name, gross_margin, period_dates[j])
    return None

def _calc_operating_margin(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Operating Margin = Operating Income / Revenue"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    revenue = data.get("revenue", [])
    
    if not isinstance(operating_income, list) or not isinstance(revenue, list):
        return None
    
    for j in range(len(period_dates) - 1, -1, -1):
        if j < len(operating_income) and j < len(revenue):
            if operating_income[j] is not None and revenue[j] is not None and revenue[j] != 0:
                operating_margin = operating_income[j] / revenue[j]
                return (symbol, company_name, operating_margin, period_dates[j])
    return None

def _calc_revenue_growth(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Revenue Growth = (Sum of last 10 quarters) / (Sum of first 10 quarters) over 20 quarters"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    if not isinstance(revenue, list) or len(revenue) < 20:
        return None
    
    for j in range(len(period_dates) - 1, 18, -1):
        if j >= 19:
            sum1 = 0.0
            sum2 = 0.0
            valid_data = True
            
            for k in range(j - 19, j - 9):
                if k < len(revenue) and revenue[k] is not None:
                    sum1 += float(revenue[k])
                else:
                    valid_data = False
                    break
            
            if valid_data:
                for k in range(j - 9, j + 1):
                    if k < len(revenue) and revenue[k] is not None:
                        sum2 += float(revenue[k])
                    else:
                        valid_data = False
                        break
            
            if valid_data and sum1 != 0:
                revenue_growth = sum2 / sum1
                return (symbol, company_name, revenue_growth, period_dates[j])
    return None

def _calc_growth_consistency(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Growth Consistency = Standard deviation of YoY revenue growth over 20 quarters"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    revenue = data.get("revenue", [])
    if not isinstance(revenue, list) or len(revenue) < 20:
        return None
    
    for j in range(len(period_dates) - 1, 18, -1):
        if j >= 19:
            yoy_growth_rates = []
            valid_data = True
            
            for k in range(4, j + 1):
                if k < len(revenue) and (k - 4) >= 0 and (k - 4) < len(revenue):
                    current_revenue = revenue[k] if revenue[k] is not None else None
                    previous_year_revenue = revenue[k - 4] if revenue[k - 4] is not None else None
                    
                    if current_revenue is not None and previous_year_revenue is not None:
                        if previous_year_revenue != 0:
                            yoy_growth = (current_revenue - previous_year_revenue) / previous_year_revenue
                            yoy_growth_rates.append(yoy_growth)
                        else:
                            continue
                    else:
                        valid_data = False
                        break
                else:
                    valid_data = False
                    break
            
            if valid_data and len(yoy_growth_rates) >= 4:
                mean_growth = sum(yoy_growth_rates) / len(yoy_growth_rates)
                variance = sum((x - mean_growth) ** 2 for x in yoy_growth_rates) / len(yoy_growth_rates)
                stdev = math.sqrt(variance)
                return (symbol, company_name, stdev, period_dates[j])
    return None

def _calc_operating_margin_consistency(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Operating Margin Consistency = Standard deviation of operating margins over 20 quarters"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    revenue = data.get("revenue", [])
    
    if not isinstance(operating_income, list) or not isinstance(revenue, list):
        return None
    
    if len(operating_income) < 20 or len(revenue) < 20:
        return None
    
    # Find the most recent position where we have 20 quarters of data
    for j in range(len(period_dates) - 1, 18, -1):
        if j >= 19:
            # Calculate operating margin for each of the last 20 quarters (indices j-19 to j)
            operating_margins = []
            valid_data = True
            
            for k in range(j - 19, j + 1):
                if k < len(operating_income) and k < len(revenue):
                    oi = operating_income[k] if operating_income[k] is not None else None
                    rev = revenue[k] if revenue[k] is not None else None
                    
                    if oi is not None and rev is not None and rev != 0:
                        operating_margin = oi / rev
                        operating_margins.append(operating_margin)
                    else:
                        # Missing data - we need all 20 quarters for consistency
                        valid_data = False
                        break
                else:
                    valid_data = False
                    break
            
            if valid_data and len(operating_margins) == 20:
                # Calculate standard deviation of operating margins
                mean_margin = sum(operating_margins) / len(operating_margins)
                variance = sum((x - mean_margin) ** 2 for x in operating_margins) / len(operating_margins)
                stdev = math.sqrt(variance)
                return (symbol, company_name, stdev, period_dates[j])
    return None

def _calc_operating_margin_growth(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Operating Margin Growth = (Operating Margin of last 10 quarters) / (Operating Margin of first 10 quarters) over 20 quarters"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    operating_income = data.get("operating_income", [])
    revenue = data.get("revenue", [])
    
    if not isinstance(operating_income, list) or not isinstance(revenue, list):
        return None
    
    if len(operating_income) < 20 or len(revenue) < 20:
        return None
    
    # Find the most recent position where we have 20 quarters of data
    for j in range(len(period_dates) - 1, 18, -1):
        if j >= 19:
            # Calculate operating margin for first 10 quarters (indices j-19 to j-10)
            sum_oi_first = 0.0
            sum_rev_first = 0.0
            valid_data = True
            
            for k in range(j - 19, j - 9):
                if k < len(operating_income) and k < len(revenue):
                    oi = operating_income[k] if operating_income[k] is not None else None
                    rev = revenue[k] if revenue[k] is not None else None
                    if oi is not None and rev is not None:
                        sum_oi_first += float(oi)
                        sum_rev_first += float(rev)
                    else:
                        valid_data = False
                        break
                else:
                    valid_data = False
                    break
            
            if not valid_data:
                continue
            
            # Calculate operating margin for last 10 quarters (indices j-9 to j)
            sum_oi_last = 0.0
            sum_rev_last = 0.0
            
            for k in range(j - 9, j + 1):
                if k < len(operating_income) and k < len(revenue):
                    oi = operating_income[k] if operating_income[k] is not None else None
                    rev = revenue[k] if revenue[k] is not None else None
                    if oi is not None and rev is not None:
                        sum_oi_last += float(oi)
                        sum_rev_last += float(rev)
                    else:
                        valid_data = False
                        break
                else:
                    valid_data = False
                    break
            
            if valid_data and sum_rev_first != 0 and sum_rev_last != 0:
                # Operating margin = Total Operating Income / Total Revenue
                operating_margin_first = sum_oi_first / sum_rev_first
                operating_margin_last = sum_oi_last / sum_rev_last
                
                # Operating margin growth = last 10 quarters margin / first 10 quarters margin
                if operating_margin_first != 0:
                    operating_margin_growth = operating_margin_last / operating_margin_first
                    return (symbol, company_name, operating_margin_growth, period_dates[j])
    return None

def _calc_net_debt_to_ttm_operating_income(stock_data: Dict) -> Optional[Tuple[str, str, float, str]]:
    """Calculate Net Debt to TTM Operating Income = Net Debt / (Sum of Operating Income for last 4 quarters)"""
    if not stock_data or "data" not in stock_data:
        return None
    
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    data = stock_data.get("data", {})
    period_dates = _get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    net_debt = data.get("net_debt", [])
    operating_income = data.get("operating_income", [])
    
    if not isinstance(net_debt, list) or not isinstance(operating_income, list):
        return None
    
    # Need at least 4 quarters of data for TTM calculation
    if len(period_dates) < 4:
        return None
    
    # Find the most recent position where we have at least 4 quarters of data
    for j in range(len(period_dates) - 1, 3, -1):
        if j < len(net_debt) and net_debt[j] is not None:
            # Get net debt from most recent quarter
            current_net_debt = net_debt[j]
            
            # Calculate TTM operating income (sum of last 4 quarters)
            ttm_operating_income = 0.0
            valid_ttm = True
            
            for k in range(max(0, j - 3), j + 1):
                if k < len(operating_income) and operating_income[k] is not None:
                    ttm_operating_income += float(operating_income[k])
                else:
                    valid_ttm = False
                    break
            
            if not valid_ttm:
                continue
            
            # Store original values for edge case checking
            original_net_debt = current_net_debt
            original_ttm_oi = ttm_operating_income
            
            # Handle edge cases to ensure consistent scoring (lower is better for reverse score)
            # Best to worst scenarios:
            # 1. Net cash (negative debt) + positive income = BEST (negative ratio, ranked best)
            # 2. Net cash + negative income = GOOD (0, ranked well)
            # 3. Zero debt + positive income = GOOD (0, ranked well)
            # 4. Zero debt + negative income = OK (0, ranked well)
            # 5. Net debt + positive income = NORMAL (positive ratio, lower is better)
            # 6. Net debt + negative income = WORST (very high value, ranked worst)
            
            # Case 1: Net cash (negative debt) + positive income = BEST
            if original_net_debt < 0 and original_ttm_oi > 0:
                # Negative ratio indicates net cash position, which is best
                # For reverse score, negative values rank best (lowest)
                ratio = original_net_debt / original_ttm_oi
                return (symbol, company_name, ratio, period_dates[j])
            
            # Case 2: Net cash + negative income = GOOD (better than debt + negative income)
            if original_net_debt < 0 and original_ttm_oi < 0:
                # Net cash position even with losses is better than debt
                # Return 0 for reverse score (ranks well)
                return (symbol, company_name, 0.0, period_dates[j])
            
            # Case 3 & 4: Zero debt scenarios
            if original_net_debt == 0:
                # No debt is good regardless of income situation
                # Return 0 for reverse score (ranks well)
                return (symbol, company_name, 0.0, period_dates[j])
            
            # Case 5: Net debt + positive income = NORMAL
            if original_net_debt > 0 and original_ttm_oi > 0:
                # Standard ratio, lower is better for reverse score
                ratio = original_net_debt / original_ttm_oi
                return (symbol, company_name, ratio, period_dates[j])
            
            # Case 6: Net debt + negative income = WORST
            if original_net_debt > 0 and original_ttm_oi < 0:
                # Having debt while losing money is the worst scenario
                # Return a very high value so it ranks worst in reverse score
                # Use a large multiplier to ensure it's always worse than normal ratios
                # abs(operating_income) to get magnitude, then invert and multiply by net_debt
                # This ensures higher debt and larger losses = worse score
                worst_value = original_net_debt / abs(original_ttm_oi) * 1000
                return (symbol, company_name, worst_value, period_dates[j])
    return None

# ============================================================================
# METRIC REGISTRY - ADD NEW METRICS HERE
# ============================================================================

METRICS: List[MetricConfig] = [
    MetricConfig(
        key="ebit_ppe",
        display_name="EBIT/PPE",
        description="EBIT/PPE = Operating Income / PPE (most recent quarter)",
        calculator=_calc_ebit_ppe,
        sort_descending=True,
        include_in_total=True
    ),
    MetricConfig(
        key="gross_margin",
        display_name="Gross Margin",
        description="Gross Margin = (Revenue - Cost of Goods Sold) / Revenue (most recent quarter)",
        calculator=_calc_gross_margin,
        sort_descending=True,
        include_in_total=True
    ),
    MetricConfig(
        key="operating_margin",
        display_name="Operating Margin",
        description="Operating Margin = Operating Income / Revenue (most recent quarter)",
        calculator=_calc_operating_margin,
        sort_descending=True,
        include_in_total=True
    ),
    MetricConfig(
        key="revenue_growth",
        display_name="Revenue Growth",
        description="Revenue Growth = (Sum of last 10 quarters revenue) / (Sum of first 10 quarters revenue) over 20 quarters (5 years)",
        calculator=_calc_revenue_growth,
        sort_descending=True,
        include_in_total=True
    ),
    MetricConfig(
        key="growth_consistency",
        display_name="Growth Consistency",
        description="Growth Consistency = Standard deviation of year-over-year revenue growth rates over 20 quarters (5 years). Lower is better (more consistent).",
        calculator=_calc_growth_consistency,
        sort_descending=False,  # Lower stdev is better
        include_in_total=True
    ),
    MetricConfig(
        key="operating_margin_growth",
        display_name="Operating Margin Growth",
        description="Operating Margin Growth = (Operating Margin of last 10 quarters) / (Operating Margin of first 10 quarters) over 20 quarters (5 years). Operating Margin = Total Operating Income / Total Revenue for each 10-quarter period.",
        calculator=_calc_operating_margin_growth,
        sort_descending=True,
        include_in_total=True
    ),
    MetricConfig(
        key="operating_margin_consistency",
        display_name="Operating Margin Consistency",
        description="Operating Margin Consistency = Standard deviation of operating margins over 20 quarters (5 years). Lower is better (more consistent).",
        calculator=_calc_operating_margin_consistency,
        sort_descending=False,  # Lower stdev is better
        include_in_total=True
    ),
    MetricConfig(
        key="net_debt_to_ttm_operating_income",
        display_name="Net Debt to TTM Operating Income",
        description="Net Debt to TTM Operating Income = Net Debt / (Sum of Operating Income for last 4 quarters). Lower is better. Edge cases: negative net debt set to 0, negative operating income set to 1000, both negative set to 0.",
        calculator=_calc_net_debt_to_ttm_operating_income,
        sort_descending=False,  # Lower debt is better
        include_in_total=True
    ),
]

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def load_data_from_jsonl(filename: str) -> List[Dict]:
    """Load stock data from JSONL file"""
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found")
        return []
    
    stocks = []
    try:
        with open(filename, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
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

def calculate_percentile(rank: int, total: int) -> float:
    """Calculate percentile rank (0-100) for a given rank"""
    if total == 0:
        return 0.0
    if total == 1:
        return 100.0
    return (total - rank + 1) / total * 100.0

def calculate_scores_for_all_stocks(nyse_stocks: List[Dict], nasdaq_stocks: List[Dict]) -> List[Dict]:
    """Calculate all metrics and percentiles for all stocks"""
    all_stock_data = []
    
    # Process NYSE stocks
    print(f"Processing {len(nyse_stocks)} NYSE stocks...")
    for stock_data in nyse_stocks:
        stock_entry = _process_stock(stock_data, "NYSE")
        if stock_entry:
            all_stock_data.append(stock_entry)
    
    # Process NASDAQ stocks
    print(f"Processing {len(nasdaq_stocks)} NASDAQ stocks...")
    for stock_data in nasdaq_stocks:
        stock_entry = _process_stock(stock_data, "NASDAQ")
        if stock_entry:
            all_stock_data.append(stock_entry)
    
    # Rank and calculate percentiles for each metric
    for metric in METRICS:
        _rank_metric(all_stock_data, metric)
    
    # Calculate total percentile for stocks with all required metrics
    _calculate_total_percentile(all_stock_data)
    
    return all_stock_data

def _process_stock(stock_data: Dict, exchange: str) -> Optional[Dict]:
    """Process a single stock and calculate all metrics"""
    symbol = stock_data.get("symbol")
    company_name = stock_data.get("company_name", symbol)
    
    stock_entry = {
        "symbol": symbol,
        "company_name": company_name,
        "exchange": exchange,
        "period": None,
        "market_cap": None
    }
    
    # Extract market cap from most recent quarter
    if stock_data and "data" in stock_data:
        data = stock_data.get("data", {})
        market_caps = data.get("market_cap", [])
        if isinstance(market_caps, list) and len(market_caps) > 0:
            # Get the most recent market cap (last element)
            for j in range(len(market_caps) - 1, -1, -1):
                if j < len(market_caps) and market_caps[j] is not None:
                    stock_entry["market_cap"] = market_caps[j]
                    break
    
    # Initialize all metrics to None
    for metric in METRICS:
        stock_entry[metric.key] = None
    
    # Calculate all metrics
    has_any_metric = False
    for metric in METRICS:
        result = metric.calculator(stock_data)
        if result:
            _, _, value, period = result
            stock_entry[metric.key] = value
            if stock_entry["period"] is None:
                stock_entry["period"] = period
            has_any_metric = True
    
    return stock_entry if has_any_metric else None

def _rank_metric(all_stock_data: List[Dict], metric: MetricConfig):
    """Rank stocks by a specific metric and calculate percentiles"""
    # Get stocks with this metric
    metric_stocks = [s for s in all_stock_data if s[metric.key] is not None]
    
    # Sort by metric value
    metric_stocks.sort(key=lambda x: x[metric.key], reverse=metric.sort_descending)
    
    # Assign ranks and percentiles
    total = len(metric_stocks)
    for rank, stock in enumerate(metric_stocks, start=1):
        stock[f"{metric.key}_rank"] = rank
        stock[f"{metric.key}_percentile"] = calculate_percentile(rank, total)
    
    # Set None for stocks without this metric
    for stock in all_stock_data:
        if stock[metric.key] is None:
            stock[f"{metric.key}_rank"] = None
            stock[f"{metric.key}_percentile"] = None

def _calculate_total_percentile(all_stock_data: List[Dict]):
    """Calculate total percentile based on metrics that are included in total"""
    # Get metrics that should be included in total
    total_metrics = [m for m in METRICS if m.include_in_total]
    
    # Find stocks that have all required metrics
    stocks_with_all = []
    for stock in all_stock_data:
        if all(stock[m.key] is not None for m in total_metrics):
            stocks_with_all.append(stock)
    
    # Calculate average rank for each stock
    for stock in stocks_with_all:
        ranks = [stock[f"{m.key}_rank"] for m in total_metrics]
        avg_rank = sum(ranks) / len(ranks)
        stock["_combined_rank"] = avg_rank
    
    # Sort by combined rank
    stocks_with_all.sort(key=lambda x: x["_combined_rank"])
    
    # Assign total ranks and percentiles
    total_stocks = len(stocks_with_all)
    for rank, stock in enumerate(stocks_with_all, start=1):
        stock["total_rank"] = rank
        stock["total_percentile"] = calculate_percentile(rank, total_stocks)
    
    # Set None for stocks without all metrics
    for stock in all_stock_data:
        if stock not in stocks_with_all:
            stock["total_rank"] = None
            stock["total_percentile"] = None
    
    # Clean up temporary field
    for stock in all_stock_data:
        stock.pop("_combined_rank", None)

def save_scores_to_json(scores_data: List[Dict], filename: str = "scores.json"):
    """Save calculated scores to JSON file"""
    try:
        # Count stocks with each metric
        metric_counts = {}
        for metric in METRICS:
            metric_counts[metric.key] = sum(1 for s in scores_data if s.get(metric.key) is not None)
        
        total_metrics = [m for m in METRICS if m.include_in_total]
        stocks_with_all = sum(1 for s in scores_data if all(s.get(m.key) is not None for m in total_metrics))
        
        metrics_dict = {m.key: m.description for m in METRICS}
        metrics_dict["total_percentile"] = f"Combined percentile based on average rank of {', '.join([m.display_name for m in total_metrics])}"
        
        output_data = {
            "metadata": {
                "total_stocks": len(scores_data),
                "calculation_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "metrics": metrics_dict,
                "description": "Percentiles where 100 is highest value, 0 is lowest. Total percentile combines ranks from included metrics."
            },
            "scores": scores_data
        }
        
        # Add metric counts to metadata
        for metric in METRICS:
            output_data["metadata"][f"stocks_with_{metric.key}"] = metric_counts[metric.key]
        output_data["metadata"]["stocks_with_all_metrics"] = stocks_with_all
        
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\nScores saved to {filename}")
        print(f"Saved scores for {len(scores_data)} stock(s)")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def load_scores_from_json(filename: str = "scores.json") -> Optional[Dict]:
    """Load scores from JSON file"""
    if not os.path.exists(filename):
        return None
    
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return None

def lookup_stock(symbol: str, filename: str = "scores.json") -> Optional[Dict]:
    """Look up a stock's percentile rank by symbol"""
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
    """Display stock percentile rank information"""
    print(f"\n{'='*80}")
    print(f"Stock: {stock['symbol']} - {stock['company_name']}")
    print(f"{'='*80}")
    print(f"Exchange: {stock.get('exchange', 'N/A')}")
    print(f"Period: {stock.get('period', 'N/A')}")
    
    # Display each metric
    for metric in METRICS:
        value = stock.get(metric.key)
        if value is not None:
            print(f"\n{metric.display_name}: {value:.4f}")
            rank = stock.get(f"{metric.key}_rank")
            percentile = stock.get(f"{metric.key}_percentile")
            if rank is not None:
                print(f"  Rank: {rank:,}")
                print(f"  Percentile: {percentile:.2f}")
        else:
            print(f"\n{metric.display_name}: N/A")
    
    # Total Percentile
    total_metrics = [m for m in METRICS if m.include_in_total]
    if stock.get('total_percentile') is not None:
        print(f"\nTotal Percentile: {stock['total_percentile']:.2f}")
        if stock.get('total_rank') is not None:
            print(f"  Total Rank: {stock['total_rank']:,}")
    else:
        metric_names = ', '.join([m.display_name for m in total_metrics])
        print(f"\nTotal Percentile: N/A (requires {metric_names})")
    
    print(f"{'='*80}\n")

def run_calculate_command():
    """Execute the 'calc' command to calculate and save scores for all stocks"""
    program_start_time = time.time()
    print("Calculating Stock Scores and Percentiles")
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
    print("\nCalculating scores and percentiles...")
    start_time = time.time()
    scores_data = calculate_scores_for_all_stocks(nyse_stocks, nasdaq_stocks)
    elapsed_time = time.time() - start_time
    print(f"Score calculation completed in {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    
    if not scores_data:
        print("\nNo scores were successfully calculated.")
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
        # Print statistics for each metric
        for metric in METRICS:
            values = [s[metric.key] for s in scores_data if s.get(metric.key) is not None]
            if values:
                print(f"\n{metric.display_name} Statistics ({len(values)} stocks):")
                print(f"  {'Highest' if metric.sort_descending else 'Lowest'}: {max(values):.4f}")
                print(f"  {'Lowest' if metric.sort_descending else 'Highest'}: {min(values):.4f}")
                print(f"  Median: {sorted(values)[len(values)//2]:.4f}")
                print(f"  Mean: {sum(values)/len(values):.4f}")
        
        # Count by exchange
        nyse_count = sum(1 for s in scores_data if s.get("exchange") == "NYSE")
        nasdaq_count = sum(1 for s in scores_data if s.get("exchange") == "NASDAQ")
        print(f"\nBy Exchange:")
        print(f"  NYSE: {nyse_count} stocks")
        print(f"  NASDAQ: {nasdaq_count} stocks")
    
    total_time = time.time() - program_start_time
    print(f"{'='*80}")
    print(f"Total program execution time: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")
    print(f"{'='*80}")

def run_lookup_command(symbol: str):
    """Execute the stock lookup command"""
    stock = lookup_stock(symbol)
    if stock:
        display_stock_info(stock)
    else:
        print(f"\nStock '{symbol}' not found in scores.json")
        print("Make sure you've run 'calc' first, and that the stock symbol is correct.\n")

def run_multi_lookup_command(symbols: List[str]):
    """Execute multi-stock lookup command - shows total percentiles ranked"""
    scores_data = load_scores_from_json()
    if not scores_data:
        print(f"Error: scores.json not found. Please run 'calc' command first.\n")
        return
    
    scores = scores_data.get("scores", [])
    if not scores:
        print("No stock scores found in scores.json\n")
        return
    
    # Look up each symbol
    found_stocks = []
    not_found = []
    
    for symbol in symbols:
        symbol_upper = symbol.upper().strip()
        if not symbol_upper:
            continue
        
        stock = None
        for s in scores:
            if s.get("symbol", "").upper() == symbol_upper:
                stock = s
                break
        
        if stock:
            found_stocks.append(stock)
        else:
            not_found.append(symbol_upper)
    
    if not found_stocks:
        print(f"\nNone of the provided symbols were found in scores.json")
        if not_found:
            print(f"Not found: {', '.join(not_found)}")
        print("Make sure you've run 'calc' first, and that the stock symbols are correct.\n")
        return
    
    # Sort by total percentile (descending)
    def get_sort_key(stock):
        if stock.get("total_percentile") is not None:
            return (0, -stock["total_percentile"])
        # Fallback to first available metric percentile
        for metric in METRICS:
            pct = stock.get(f"{metric.key}_percentile")
            if pct is not None:
                return (1, -pct)
        return (2, 0)
    
    sorted_stocks = sorted(found_stocks, key=get_sort_key)
    
    # Display results
    print(f"\n{'='*80}")
    print(f"Stock Comparison - Ranked by Total Percentile")
    print(f"{'='*80}")
    print(f"{'Rank':<8} {'Symbol':<12} {'Company Name':<30} {'Total %':<12} {'Exchange':<10}")
    print(f"{'-'*80}")
    
    for idx, stock in enumerate(sorted_stocks, start=1):
        symbol = stock.get("symbol", "N/A")
        company_name = stock.get("company_name", "N/A")
        if len(company_name) > 28:
            company_name = company_name[:25] + "..."
        
        total_pct = f"{stock.get('total_percentile', 0):.2f}" if stock.get("total_percentile") is not None else "N/A"
        exchange = stock.get("exchange", "N/A")
        
        print(f"{idx:<8} {symbol:<12} {company_name:<30} {total_pct:<12} {exchange:<10}")
    
    print(f"{'='*80}")
    
    if not_found:
        print(f"\nNote: The following symbols were not found: {', '.join(not_found)}")
    
    print()

def run_view_command(limit: Optional[int] = None, min_market_cap: Optional[float] = None):
    """Display all stocks ranked by total percentile, optionally filtered by market cap"""
    scores_data = load_scores_from_json()
    if not scores_data:
        print(f"Error: scores.json not found. Please run 'calc' command first.\n")
        return
    
    scores = scores_data.get("scores", [])
    if not scores:
        print("No stock scores found in scores.json\n")
        return
    
    # Filter by market cap if specified (market cap is in dollars, min_market_cap is in billions)
    if min_market_cap is not None:
        min_market_cap_dollars = min_market_cap * 1_000_000_000  # Convert billions to dollars
        scores = [s for s in scores if s.get("market_cap") is not None and s.get("market_cap") >= min_market_cap_dollars]
        if not scores:
            print(f"No stocks found with market cap over ${min_market_cap}B\n")
            return
    
    # Sort by total percentile
    def get_sort_key(stock):
        if stock.get("total_percentile") is not None:
            return (0, -stock["total_percentile"])
        # Fallback to first available metric percentile
        for metric in METRICS:
            pct = stock.get(f"{metric.key}_percentile")
            if pct is not None:
                return (1, -pct)
        return (2, 0)
    
    sorted_scores = sorted(scores, key=get_sort_key)
    
    if limit:
        sorted_scores = sorted_scores[:limit]
    
    # Build header
    header_parts = ['Rank', 'Symbol', 'Company Name', 'Total %', 'Exchange']
    if min_market_cap is not None:
        header_parts.append('Market Cap (B)')
    
    print(f"\n{'='*80}")
    title = "All Stocks Ranked by Percentile"
    if limit:
        title += f" (showing top {limit})"
    if min_market_cap is not None:
        title += f" (market cap > ${min_market_cap}B)"
    print(title)
    print(f"{'='*80}")
    print(' '.join(f"{h:<15}" for h in header_parts))
    print(f"{'-'*80}")
    
    for idx, stock in enumerate(sorted_scores, start=1):
        row_parts = [str(idx), stock.get("symbol", "N/A")]
        company_name = stock.get("company_name", "N/A")
        if len(company_name) > 13:
            company_name = company_name[:10] + "..."
        row_parts.append(company_name)
        
        total_pct = f"{stock.get('total_percentile', 0):.2f}" if stock.get("total_percentile") is not None else "N/A"
        row_parts.append(total_pct)
        
        row_parts.append(stock.get("exchange", "N/A"))
        
        if min_market_cap is not None:
            market_cap = stock.get("market_cap")
            if market_cap is not None:
                market_cap_b = market_cap / 1_000_000_000
                row_parts.append(f"${market_cap_b:.2f}")
            else:
                row_parts.append("N/A")
        
        print(' '.join(f"{p:<15}" for p in row_parts))
    
    print(f"{'='*80}")
    print(f"\nTotal stocks displayed: {len(sorted_scores)}")
    if limit and len(scores) > limit:
        print(f"Total stocks in database: {len(scores)}")
        print(f"Use 'view' without a number to see all stocks, or 'view <number>' to see top N stocks.\n")
    else:
        print()

def run_metrics_command():
    """Display all current metrics being calculated"""
    print("\n" + "=" * 80)
    print("Current Metrics")
    print("=" * 80)
    
    total_metrics = [m for m in METRICS if m.include_in_total]
    
    print(f"\nTotal Metrics: {len(METRICS)}")
    print(f"Metrics Included in Total Percentile: {len(total_metrics)}")
    
    print(f"\n{'='*80}")
    print("Metric Details:")
    print(f"{'='*80}")
    
    for idx, metric in enumerate(METRICS, start=1):
        print(f"\n{idx}. {metric.display_name}")
        print(f"   Key: {metric.key}")
        print(f"   Description: {metric.description}")
        print(f"   Sort Direction: {'Descending (Higher is better)' if metric.sort_descending else 'Ascending (Lower is better)'}")
        print(f"   Included in Total Percentile: {'Yes' if metric.include_in_total else 'No'}")
    
    if total_metrics:
        print(f"\n{'='*80}")
        print("Total Percentile Calculation:")
        print(f"{'='*80}")
        print(f"The total percentile is calculated as the average rank of the following metrics:")
        for idx, metric in enumerate(total_metrics, start=1):
            print(f"  {idx}. {metric.display_name}")
    
    print(f"\n{'='*80}\n")

def run_clear_command():
    """Clear the screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_help():
    """Print help message with available commands"""
    print("\n" + "=" * 80)
    print("Available Commands:")
    print("=" * 80)
    print("  calc                   - Calculate and save scores for all stocks")
    print("  view [N]               - View all stocks ranked by percentile (optionally show top N)")
    print("  view [N] over [X]      - View top N stocks with market cap over X billion (e.g., 'view 50 over 10')")
    print("  metrics                - Show all current metrics being calculated")
    print("  <symbol>               - Look up percentile rank for a stock (e.g., AAPL, MSFT)")
    print("  <symbol> <symbol> ...  - Compare multiple stocks ranked by total percentile (e.g., AAPL MSFT GOOGL)")
    print("  help                   - Show this help message")
    print("  clear                  - Clear the screen")
    print("  exit                   - Exit the program")
    print("=" * 80 + "\n")

def main():
    """Main function with interactive command terminal"""
    print("=" * 80)
    print("Stock Scorer - Interactive Terminal")
    print("=" * 80)
    print_help()
    
    while True:
        try:
            user_input = input("Enter command: ").strip()
            
            if not user_input:
                continue
            
            command_parts = user_input.split()
            command = command_parts[0].lower()
            
            if command == "exit":
                print("\nExiting program. Goodbye!\n")
                break
            elif command == "help":
                print_help()
            elif command == "calc":
                run_calculate_command()
                print()
            elif command == "metrics":
                run_metrics_command()
            elif command == "clear":
                run_clear_command()
            elif command == "view":
                limit = None
                min_market_cap = None
                
                if len(command_parts) > 1:
                    # Check for "over" keyword to filter by market cap
                    if "over" in command_parts:
                        over_index = command_parts.index("over")
                        
                        # Parse limit (number before "over")
                        if over_index > 1:
                            try:
                                limit = int(command_parts[1])
                                if limit <= 0:
                                    print("Limit must be a positive number. Showing all stocks.\n")
                                    limit = None
                            except ValueError:
                                print(f"Invalid limit '{command_parts[1]}'. Showing all stocks.\n")
                        
                        # Parse market cap threshold (number after "over")
                        if over_index + 1 < len(command_parts):
                            try:
                                min_market_cap = float(command_parts[over_index + 1])
                                if min_market_cap < 0:
                                    print("Market cap threshold must be non-negative. Ignoring filter.\n")
                                    min_market_cap = None
                            except ValueError:
                                print(f"Invalid market cap threshold '{command_parts[over_index + 1]}'. Ignoring filter.\n")
                    else:
                        # No "over" keyword, just parse limit
                        try:
                            limit = int(command_parts[1])
                            if limit <= 0:
                                print("Limit must be a positive number. Showing all stocks.\n")
                                limit = None
                        except ValueError:
                            print(f"Invalid limit '{command_parts[1]}'. Showing all stocks.\n")
                
                run_view_command(limit, min_market_cap)
            else:
                # Check if input contains multiple space-separated symbols
                if len(command_parts) > 1:
                    # Multiple symbols provided
                    run_multi_lookup_command(command_parts)
                else:
                    # Single symbol
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
