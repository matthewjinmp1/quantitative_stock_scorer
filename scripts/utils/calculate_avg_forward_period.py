"""
Calculate the average period (in years) of all total forward returns across all stocks.

For each stock:
- If a stock has data for N periods, it will have forward returns for periods spanning
  N-1, N-2, ..., 1 periods forward
- Each forward return period is calculated in years based on the number of quarters
  from the current period to the most recent period

The program:
1. Loads data from metrics.json
2. For each stock, for each period with a forward_return:
   - Calculates how many years the forward return spans
   - Adds this to the total
3. Divides total by count to get average
"""
import json
import os
from typing import List, Dict, Optional
from datetime import datetime


def load_metrics_data(filename: str = "metrics.json") -> List[Dict]:
    """
    Load metrics data from JSON file.
    
    Args:
        filename: Path to metrics.json file
    
    Returns:
        List of stock data dictionaries
    """
    if not os.path.exists(filename):
        print(f"Error: {filename} not found")
        return []
    
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {filename}")
        return []
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []


def parse_period_date(period: str) -> Optional[datetime]:
    """
    Parse a period string into a datetime object.
    
    Handles formats like:
    - "2020-Q1" -> datetime(2020, 1, 1)
    - "2020-01-01" -> datetime(2020, 1, 1)
    - ISO format strings
    
    Args:
        period: Period string (e.g., "2020-Q1" or date string)
    
    Returns:
        datetime object or None if parsing fails
    """
    if not period:
        return None
    
    # Try quarter format: "2020-Q1"
    if '-Q' in period:
        try:
            year_str, quarter_str = period.split('-Q')
            year = int(year_str)
            quarter = int(quarter_str)
            # Convert quarter to month (Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct)
            month = (quarter - 1) * 3 + 1
            return datetime(year, month, 1)
        except (ValueError, IndexError):
            pass
    
    # Try ISO date format: "2020-01-01"
    try:
        return datetime.fromisoformat(period.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        pass
    
    # Try other common date formats
    date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']
    for fmt in date_formats:
        try:
            return datetime.strptime(period, fmt)
        except ValueError:
            continue
    
    return None


def calculate_forward_period_quarters(current_period: str, periods: List[str]) -> Optional[int]:
    """
    Calculate how many quarters forward a forward return spans.
    
    For a stock with N quarters of data:
    - First period: N quarters forward
    - Second period: N-1 quarters forward
    - ...
    - Last period: 1 quarter forward (if it has forward return)
    
    The forward return goes from current_period+1 to the most recent period.
    We count the number of quarters from current_period+1 to the end.
    
    Args:
        current_period: The period for which we're calculating forward return
        periods: List of all periods for the stock (sorted chronologically)
    
    Returns:
        Number of quarters forward, or None if cannot calculate
    """
    if not periods or current_period not in periods:
        return None
    
    try:
        current_idx = periods.index(current_period)
        # Most recent period is the last one in the list
        most_recent_idx = len(periods) - 1
        
        # Number of quarters forward = number of periods from current+1 to most recent (inclusive)
        # If current is at index 0 and most recent is at index 3:
        # - Periods from index 1 to 3: that's 3 periods = 3 quarters forward
        # But we want: if stock has 4 quarters total, first period should show 4 quarters forward
        # So: total_quarters - current_idx = 4 - 0 = 4 quarters forward ✓
        
        total_quarters = len(periods)
        quarters_forward = total_quarters - current_idx
        
        # Must have at least 1 quarter forward (can't be the last period)
        if quarters_forward <= 0:
            return None
        
        return quarters_forward
    except (ValueError, IndexError):
        return None


def calculate_average_forward_period(metrics_data: List[Dict]) -> Dict:
    """
    Calculate the average period of all total forward returns.
    
    Args:
        metrics_data: List of stock data dictionaries from metrics.json
    
    Returns:
        Dictionary with statistics including:
        - total_periods: Total number of forward return periods
        - total_years: Sum of all forward return periods in years
        - average_years: Average forward return period in years
        - stocks_analyzed: Number of stocks analyzed
        - periods_per_stock: List of period counts per stock
    """
    total_years = 0.0
    total_periods = 0
    stocks_analyzed = 0
    periods_per_stock = []
    
    for stock in metrics_data:
        symbol = stock.get("symbol", "Unknown")
        data = stock.get("data", [])
        
        if not data:
            continue
        
        # Extract all periods for this stock
        periods = [entry.get("period") for entry in data if entry.get("period")]
        
        if not periods:
            continue
        
        # Count periods with forward_return for this stock
        stock_periods = 0
        stock_total_years = 0.0
        
        for entry in data:
            period = entry.get("period")
            forward_return = entry.get("forward_return")
            
            # Only count periods that have a forward_return value
            if period and forward_return is not None:
                # Calculate how many quarters forward this forward return spans
                quarters_forward = calculate_forward_period_quarters(period, periods)
                
                if quarters_forward is not None:
                    # Convert quarters to years
                    years = quarters_forward * 0.25
                    total_years += years
                    total_periods += 1
                    stock_total_years += years
                    stock_periods += 1
        
        if stock_periods > 0:
            stocks_analyzed += 1
            periods_per_stock.append(stock_periods)
    
    # Calculate average
    average_years = total_years / total_periods if total_periods > 0 else 0.0
    
    return {
        "total_periods": total_periods,
        "total_years": total_years,
        "average_years": average_years,
        "stocks_analyzed": stocks_analyzed,
        "periods_per_stock": periods_per_stock,
        "avg_periods_per_stock": sum(periods_per_stock) / len(periods_per_stock) if periods_per_stock else 0.0
    }


def main():
    """Main function"""
    print("=" * 80)
    print("Calculate Average Forward Return Period")
    print("=" * 80)
    print()
    
    # Load metrics data
    print("Loading data from metrics.json...")
    metrics_data = load_metrics_data("metrics.json")
    
    if not metrics_data:
        print("No data loaded. Exiting.")
        return
    
    print(f"Loaded data for {len(metrics_data)} stock(s)")
    print()
    
    # Calculate average forward period
    print("Calculating average forward return period...")
    stats = calculate_average_forward_period(metrics_data)
    
    # Display results
    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)
    print(f"Stocks analyzed: {stats['stocks_analyzed']:,}")
    print(f"Total forward return periods: {stats['total_periods']:,}")
    print(f"Total years (sum of all periods): {stats['total_years']:,.2f} years")
    print()
    print(f"Average forward return period: {stats['average_years']:.2f} years")
    print(f"Average forward return period: {stats['average_years'] * 4:.2f} quarters")
    print()
    print("Calculation method:")
    print("  For each stock, count quarters forward for each period with forward_return")
    print("  Example: Stock with 4 quarters -> forward quarters: 4, 3, 2, 1")
    print("  Example: Stock with 8 quarters -> forward quarters: 8, 7, 6, 5, 4, 3, 2, 1")
    print("  Average = sum of all forward quarters / count of periods")
    print()
    print(f"Average periods per stock: {stats['avg_periods_per_stock']:.1f}")
    if stats['periods_per_stock']:
        print(f"Min periods per stock: {min(stats['periods_per_stock'])}")
        print(f"Max periods per stock: {max(stats['periods_per_stock'])}")
    print("=" * 80)


if __name__ == "__main__":
    main()

