"""
Analyze data coverage over time from NYSE and NASDAQ data files.

Shows how much non-null data exists for each period/quarter over time,
helping to understand data availability and completeness.
"""
import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

def get_period_dates(data: Dict) -> Optional[List]:
    """Extract period dates from data dictionary"""
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            return data[date_key]
    return None

def load_data_from_jsonl(filename: str) -> List[Dict]:
    """Load stock data from JSONL file"""
    if not os.path.exists(filename):
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
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []
    
    return stocks

def analyze_data_coverage(stocks: List[Dict], exchange_name: str) -> Dict:
    """
    Analyze data coverage over time for a list of stocks
    
    Returns:
        Dictionary with coverage statistics by period
    """
    print(f"\nAnalyzing {exchange_name} data...")
    print(f"  Total stocks: {len(stocks)}")
    
    # Track coverage by period
    period_coverage = defaultdict(lambda: {
        'total_stocks': 0,
        'operating_income': 0,
        'ppe_net': 0,
        'market_cap': 0,
        'revenue': 0,
        'net_income': 0,
        'dividends': 0,
        'period_end_price': 0,
        'ebit_ppe_calculable': 0  # Both operating_income and ppe_net available
    })
    
    # Fields to track
    fields_to_track = [
        'operating_income',
        'ppe_net',
        'market_cap',
        'revenue',
        'net_income',
        'dividends',
        'period_end_price'
    ]
    
    stocks_processed = 0
    for stock in stocks:
        stocks_processed += 1
        if stocks_processed % 1000 == 0:
            print(f"  Processed {stocks_processed}/{len(stocks)} stocks...")
        
        if not stock or "data" not in stock:
            continue
        
        data = stock.get("data", {})
        period_dates = get_period_dates(data)
        
        if not period_dates or not isinstance(period_dates, list):
            continue
        
        # For each period, check what data is available
        for period_idx, date_str in enumerate(period_dates):
            date_obj = parse_date(date_str)
            if not date_obj:
                continue
            
            # Use year-month as key for grouping
            period_key = date_obj.strftime("%Y-%m")
            
            period_coverage[period_key]['total_stocks'] += 1
            
            # Check each field
            for field in fields_to_track:
                field_data = data.get(field, [])
                if isinstance(field_data, list) and period_idx < len(field_data):
                    value = field_data[period_idx]
                    if value is not None:
                        period_coverage[period_key][field] += 1
            
            # Check if EBIT/PPE is calculable (both operating_income and ppe_net available and ppe != 0)
            operating_income = data.get('operating_income', [])
            ppe_net = data.get('ppe_net', [])
            if (isinstance(operating_income, list) and isinstance(ppe_net, list) and
                period_idx < len(operating_income) and period_idx < len(ppe_net)):
                if (operating_income[period_idx] is not None and 
                    ppe_net[period_idx] is not None and 
                    ppe_net[period_idx] != 0):
                    period_coverage[period_key]['ebit_ppe_calculable'] += 1
    
    print(f"  Analysis complete for {exchange_name}")
    return dict(period_coverage)

def main():
    """Main function"""
    print("=" * 80)
    print("Data Coverage Analysis - NYSE and NASDAQ")
    print("=" * 80)
    
    # Load data
    print("\nLoading data files...")
    nyse_stocks = load_data_from_jsonl("nyse_data.jsonl")
    nasdaq_stocks = load_data_from_jsonl("nasdaq_data.jsonl")
    
    print(f"NYSE stocks: {len(nyse_stocks)}")
    print(f"NASDAQ stocks: {len(nasdaq_stocks)}")
    
    # Analyze coverage
    nyse_coverage = analyze_data_coverage(nyse_stocks, "NYSE")
    nasdaq_coverage = analyze_data_coverage(nasdaq_stocks, "NASDAQ")
    
    # Combine coverage
    all_periods = set(nyse_coverage.keys()) | set(nasdaq_coverage.keys())
    combined_coverage = {}
    
    for period in sorted(all_periods):
        nyse_data = nyse_coverage.get(period, {})
        nasdaq_data = nasdaq_coverage.get(period, {})
        
        combined = {
            'total_stocks': nyse_data.get('total_stocks', 0) + nasdaq_data.get('total_stocks', 0),
            'operating_income': nyse_data.get('operating_income', 0) + nasdaq_data.get('operating_income', 0),
            'ppe_net': nyse_data.get('ppe_net', 0) + nasdaq_data.get('ppe_net', 0),
            'market_cap': nyse_data.get('market_cap', 0) + nasdaq_data.get('market_cap', 0),
            'revenue': nyse_data.get('revenue', 0) + nasdaq_data.get('revenue', 0),
            'net_income': nyse_data.get('net_income', 0) + nasdaq_data.get('net_income', 0),
            'dividends': nyse_data.get('dividends', 0) + nasdaq_data.get('dividends', 0),
            'period_end_price': nyse_data.get('period_end_price', 0) + nasdaq_data.get('period_end_price', 0),
            'ebit_ppe_calculable': nyse_data.get('ebit_ppe_calculable', 0) + nasdaq_data.get('ebit_ppe_calculable', 0),
        }
        combined_coverage[period] = combined
    
    # Print summary statistics
    print("\n" + "=" * 80)
    print("DATA COVERAGE SUMMARY")
    print("=" * 80)
    
    if not combined_coverage:
        print("No data found!")
        return
    
    # Find date range
    sorted_periods = sorted(combined_coverage.keys())
    print(f"\nDate Range: {sorted_periods[0]} to {sorted_periods[-1]}")
    print(f"Total Periods: {len(sorted_periods)}")
    
    # Show coverage for key metrics
    print("\nCoverage by Period (showing sample periods):")
    print(f"{'Period':<12} {'Stocks':<8} {'Op Income':<12} {'PPE':<8} {'Market Cap':<12} {'Revenue':<10} {'EBIT/PPE':<10}")
    print("-" * 80)
    
    # Show first 10, middle 10, and last 10 periods
    sample_indices = list(range(0, min(10, len(sorted_periods))))
    if len(sorted_periods) > 20:
        sample_indices.extend(range(len(sorted_periods) // 2 - 5, len(sorted_periods) // 2 + 5))
    sample_indices.extend(range(max(0, len(sorted_periods) - 10), len(sorted_periods)))
    sample_indices = sorted(set(sample_indices))
    
    for idx in sample_indices:
        period = sorted_periods[idx]
        data = combined_coverage[period]
        total = data['total_stocks']
        if total > 0:
            print(f"{period:<12} {total:<8} {data['operating_income']:<12} {data['ppe_net']:<8} "
                  f"{data['market_cap']:<12} {data['revenue']:<10} {data['ebit_ppe_calculable']:<10}")
    
    # Calculate overall statistics
    print("\n" + "=" * 80)
    print("OVERALL STATISTICS")
    print("=" * 80)
    
    total_stocks_all_periods = sum(d['total_stocks'] for d in combined_coverage.values())
    total_op_income = sum(d['operating_income'] for d in combined_coverage.values())
    total_ppe = sum(d['ppe_net'] for d in combined_coverage.values())
    total_market_cap = sum(d['market_cap'] for d in combined_coverage.values())
    total_revenue = sum(d['revenue'] for d in combined_coverage.values())
    total_ebit_ppe = sum(d['ebit_ppe_calculable'] for d in combined_coverage.values())
    
    print(f"\nTotal stock-period observations: {total_stocks_all_periods:,}")
    print(f"Operating Income coverage: {total_op_income:,} ({total_op_income/total_stocks_all_periods*100:.1f}%)")
    print(f"PPE Net coverage: {total_ppe:,} ({total_ppe/total_stocks_all_periods*100:.1f}%)")
    print(f"Market Cap coverage: {total_market_cap:,} ({total_market_cap/total_stocks_all_periods*100:.1f}%)")
    print(f"Revenue coverage: {total_revenue:,} ({total_revenue/total_stocks_all_periods*100:.1f}%)")
    print(f"EBIT/PPE calculable: {total_ebit_ppe:,} ({total_ebit_ppe/total_stocks_all_periods*100:.1f}%)")
    
    # Create visualization
    print("\n" + "=" * 80)
    print("Creating coverage charts...")
    
    # Prepare data for plotting
    dates = [parse_date(p + "-01") for p in sorted_periods]
    dates = [d for d in dates if d is not None]
    
    if not dates:
        print("No valid dates found for plotting")
        return
    
    # Get corresponding coverage percentages
    stocks_count = [combined_coverage[p]['total_stocks'] for p in sorted_periods[:len(dates)]]
    op_income_pct = [combined_coverage[p]['operating_income'] / max(combined_coverage[p]['total_stocks'], 1) * 100 
                     for p in sorted_periods[:len(dates)]]
    ppe_pct = [combined_coverage[p]['ppe_net'] / max(combined_coverage[p]['total_stocks'], 1) * 100 
               for p in sorted_periods[:len(dates)]]
    market_cap_pct = [combined_coverage[p]['market_cap'] / max(combined_coverage[p]['total_stocks'], 1) * 100 
                      for p in sorted_periods[:len(dates)]]
    revenue_pct = [combined_coverage[p]['revenue'] / max(combined_coverage[p]['total_stocks'], 1) * 100 
                   for p in sorted_periods[:len(dates)]]
    ebit_ppe_pct = [combined_coverage[p]['ebit_ppe_calculable'] / max(combined_coverage[p]['total_stocks'], 1) * 100 
                    for p in sorted_periods[:len(dates)]]
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 1, figsize=(16, 10))
    
    # Plot 1: Number of stocks over time
    axes[0].plot(dates, stocks_count, linewidth=2, color='#2E86AB', label='Total Stocks')
    axes[0].set_title('Number of Stocks with Data Over Time', fontsize=14, fontweight='bold')
    axes[0].set_xlabel('Date', fontsize=12)
    axes[0].set_ylabel('Number of Stocks', fontsize=12)
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()
    axes[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    axes[0].xaxis.set_major_locator(mdates.YearLocator(2))
    plt.setp(axes[0].xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 2: Coverage percentages over time
    axes[1].plot(dates, op_income_pct, linewidth=2, label='Operating Income', alpha=0.8)
    axes[1].plot(dates, ppe_pct, linewidth=2, label='PPE Net', alpha=0.8)
    axes[1].plot(dates, market_cap_pct, linewidth=2, label='Market Cap', alpha=0.8)
    axes[1].plot(dates, revenue_pct, linewidth=2, label='Revenue', alpha=0.8)
    axes[1].plot(dates, ebit_ppe_pct, linewidth=2, label='EBIT/PPE Calculable', alpha=0.8, linestyle='--')
    axes[1].set_title('Data Coverage Percentage Over Time', fontsize=14, fontweight='bold')
    axes[1].set_xlabel('Date', fontsize=12)
    axes[1].set_ylabel('Coverage (%)', fontsize=12)
    axes[1].set_ylim(0, 105)
    axes[1].grid(True, alpha=0.3)
    axes[1].legend(loc='best')
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    axes[1].xaxis.set_major_locator(mdates.YearLocator(2))
    plt.setp(axes[1].xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    plt.savefig('data_coverage_analysis.png', dpi=300, bbox_inches='tight')
    print("   Chart saved to data_coverage_analysis.png")
    plt.show()
    
    # Save detailed results to JSON
    print("\nSaving detailed results to JSON...")
    results = {
        'summary': {
            'date_range': f"{sorted_periods[0]} to {sorted_periods[-1]}",
            'total_periods': len(sorted_periods),
            'total_observations': total_stocks_all_periods,
            'coverage_percentages': {
                'operating_income': total_op_income/total_stocks_all_periods*100,
                'ppe_net': total_ppe/total_stocks_all_periods*100,
                'market_cap': total_market_cap/total_stocks_all_periods*100,
                'revenue': total_revenue/total_stocks_all_periods*100,
                'ebit_ppe_calculable': total_ebit_ppe/total_stocks_all_periods*100,
            }
        },
        'period_coverage': {
            period: {
                'total_stocks': data['total_stocks'],
                'coverage': {
                    'operating_income': data['operating_income'],
                    'ppe_net': data['ppe_net'],
                    'market_cap': data['market_cap'],
                    'revenue': data['revenue'],
                    'net_income': data['net_income'],
                    'dividends': data['dividends'],
                    'period_end_price': data['period_end_price'],
                    'ebit_ppe_calculable': data['ebit_ppe_calculable'],
                },
                'coverage_percentages': {
                    'operating_income': data['operating_income'] / max(data['total_stocks'], 1) * 100,
                    'ppe_net': data['ppe_net'] / max(data['total_stocks'], 1) * 100,
                    'market_cap': data['market_cap'] / max(data['total_stocks'], 1) * 100,
                    'revenue': data['revenue'] / max(data['total_stocks'], 1) * 100,
                    'ebit_ppe_calculable': data['ebit_ppe_calculable'] / max(data['total_stocks'], 1) * 100,
                }
            }
            for period, data in combined_coverage.items()
        }
    }
    
    with open('data_coverage_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("   Results saved to data_coverage_results.json")
    print("\n" + "=" * 80)
    print("Analysis Complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()

