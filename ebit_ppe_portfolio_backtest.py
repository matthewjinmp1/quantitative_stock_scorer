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
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import defaultdict

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

def get_market_cap_at_date(stock_data: Dict, target_year: int = 2000) -> Optional[Tuple[float, str]]:
    """Get market cap for a stock at a specific date"""
    if not stock_data or "data" not in stock_data:
        return None
    
    data = stock_data.get("data", {})
    period_dates = get_period_dates(data)
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return None
    
    market_caps = data.get("market_cap", [])
    if not isinstance(market_caps, list):
        return None
    
    # Find quarter near target year (allow earlier dates)
    quarter_idx = find_quarter_near_date(period_dates, target_year, allow_earlier=True)
    if quarter_idx is None:
        return None
    
    # Try to get data at that quarter, or nearby quarters (search wider range)
    for offset in [0, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5]:
        idx = quarter_idx + offset
        if 0 <= idx < len(period_dates) and idx < len(market_caps):
            if market_caps[idx] is not None and market_caps[idx] > 0:
                return (market_caps[idx], period_dates[idx])
    
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

def main():
    """Main function"""
    print("=" * 80)
    print("EBIT/PPE Weighted Portfolio Backtest (Starting 2002)")
    print("=" * 80)
    
    # Instead of using current S&P 500 list, we'll find stocks that:
    # 1. Have data available around 2002 (when data coverage significantly increases)
    # 2. Were large cap at that time (top 500 by market cap)
    # This approximates the S&P 500 at that time
    
    print("\n1. Finding S&P 500-like stocks from 2002...")
    print("   (Using stocks with data from 2002, ranked by market cap)")
    print("   (2002 is when data coverage significantly increases)")
    
    # Load ALL stock data first
    print("   Loading all stock data...")
    all_stocks_list = load_data_from_jsonl("nyse_data.jsonl") + load_data_from_jsonl("nasdaq_data.jsonl")
    print(f"   Loaded {len(all_stocks_list)} stocks")
    
    # Find stocks with data around 2002 and get their market caps
    print("   Finding stocks with data from 2002...")
    stocks_with_data = []
    
    for stock_data in all_stocks_list:
        # Try to get market cap and EBIT/PPE around 2002
        market_cap_result = None
        ebit_ppe_result = None
        
        # Try years 2002-2003 (focus on 2002 when data coverage jumps)
        for year in range(2002, 2004):
            if not market_cap_result:
                market_cap_result = get_market_cap_at_date(stock_data, year)
            if not ebit_ppe_result:
                ebit_ppe_result = get_ebit_ppe_at_date(stock_data, year)
            if market_cap_result and ebit_ppe_result:
                break
        
        if market_cap_result and ebit_ppe_result:
            market_cap, mc_date = market_cap_result
            ebit_ppe, ebit_date = ebit_ppe_result
            
            # Only include stocks with meaningful market cap (at least $1B to approximate S&P 500)
            if market_cap >= 1_000_000_000:  # $1B minimum
                stocks_with_data.append({
                    'stock_data': stock_data,
                    'ticker': stock_data.get("symbol", "").upper(),
                    'market_cap': market_cap,
                    'ebit_ppe': ebit_ppe,
                    'ebit_date': ebit_date,
                    'mc_date': mc_date
                })
    
    print(f"   Found {len(stocks_with_data)} stocks with data and market cap >= $1B")
    
    # Sort by market cap and take top 500 (approximate S&P 500)
    stocks_with_data.sort(key=lambda x: x['market_cap'], reverse=True)
    stock_info = stocks_with_data[:500]
    
    print(f"   Selected top {len(stock_info)} stocks by market cap (S&P 500 approximation)")
    
    # Create stock_dict for return calculations
    stock_dict = {s['ticker']: s['stock_data'] for s in stock_info}
    
    # We already have stock_info with market cap and EBIT/PPE from 2002
    print("\n2. Using market cap and EBIT/PPE from 2002 period...")
    
    # Track earliest year
    earliest_year_found = None
    for stock in stock_info:
        ebit_date_obj = parse_date(stock['ebit_date'])
        if ebit_date_obj:
            if earliest_year_found is None or ebit_date_obj.year < earliest_year_found:
                earliest_year_found = ebit_date_obj.year
    
    if earliest_year_found:
        print(f"   Using data from {len(stock_info)} stocks (earliest data from {earliest_year_found})")
    else:
        print(f"   Using data from {len(stock_info)} stocks")
    
    if not stock_info:
        print("   Error: No stocks found with EBIT/PPE and market cap data for 2000")
        return
    
    # Rank by EBIT/PPE
    print("\n3. Ranking stocks by EBIT/PPE...")
    stock_info.sort(key=lambda x: x['ebit_ppe'], reverse=True)
    
    # Calculate initial market cap weights
    total_market_cap = sum(s['market_cap'] for s in stock_info)
    for stock in stock_info:
        stock['initial_weight'] = (stock['market_cap'] / total_market_cap) * 100
    
    # Apply multiplier based on rank (0.5 for worst, 2.0 for best)
    print("\n4. Applying EBIT/PPE-based weight adjustments...")
    n_stocks = len(stock_info)
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
    
    print(f"   Total adjusted weight before normalization: {total_adjusted:.2f}%")
    print(f"   Total final weight after normalization: {sum(s['final_weight'] for s in stock_info):.2f}%")
    
    # Show top 10 and bottom 10
    print("\n   Top 10 by EBIT/PPE:")
    for i, stock in enumerate(stock_info[:10], 1):
        print(f"   {i:2d}. {stock['ticker']:6s} - EBIT/PPE: {stock['ebit_ppe']:8.4f}, "
              f"MC: ${stock['market_cap']/1e9:6.2f}B, "
              f"Initial: {stock['initial_weight']:5.2f}%, "
              f"Final: {stock['final_weight']:5.2f}%")
    
    print("\n   Bottom 10 by EBIT/PPE:")
    for i, stock in enumerate(stock_info[-10:], n_stocks - 9):
        print(f"   {i:2d}. {stock['ticker']:6s} - EBIT/PPE: {stock['ebit_ppe']:8.4f}, "
              f"MC: ${stock['market_cap']/1e9:6.2f}B, "
              f"Initial: {stock['initial_weight']:5.2f}%, "
              f"Final: {stock['final_weight']:5.2f}%")
    
    # Display all stocks ranked by final weight
    print("\n" + "=" * 80)
    print("ALL STOCKS RANKED BY FINAL WEIGHT")
    print("=" * 80)
    
    # Sort by final weight (descending)
    stocks_by_weight = sorted(stock_info, key=lambda x: x['final_weight'], reverse=True)
    
    print(f"\n{'Rank':<6} {'Ticker':<8} {'Final Weight %':<15} {'Market Cap (B)':<15} {'EBIT/PPE':<12} {'Initial Weight %':<15}")
    print("-" * 80)
    
    for rank, stock in enumerate(stocks_by_weight, 1):
        print(f"{rank:<6} {stock['ticker']:<8} {stock['final_weight']:>13.4f}% "
              f"${stock['market_cap']/1e9:>13.2f}B {stock['ebit_ppe']:>11.4f} "
              f"{stock['initial_weight']:>14.4f}%")
    
    print("\n" + "=" * 80)
    
    # Calculate total returns with dividends reinvested
    print("\n5. Calculating total returns with dividends reinvested...")
    
    # Collect all return data by date
    all_dates = set()
    stock_returns_by_date = {}  # ticker -> {date: return_pct}
    
    for stock in stock_info:
        # Use the actual date from EBIT/PPE calculation as start date
        start_date = stock.get('ebit_date')
        returns = calculate_total_return_with_dividends(stock['stock_data'], 2002, start_date)
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
        # Calculate EBIT/PPE weighted portfolio value
        portfolio_value_ebit = 0.0
        # Calculate market cap weighted portfolio value
        portfolio_value_mc = 0.0
        
        for stock in stock_info:
            ticker = stock['ticker']
            ebit_weight = stock['final_weight'] / 100.0  # EBIT/PPE adjusted weight
            mc_weight = stock['initial_weight'] / 100.0   # Market cap weight
            
            if ticker in stock_returns_by_date and date in stock_returns_by_date[ticker]:
                # Get the cumulative return for this stock at this date
                stock_return_pct = stock_returns_by_date[ticker][date]
                stock_value_multiplier = 1.0 + (stock_return_pct / 100.0)
                portfolio_value_ebit += ebit_weight * stock_value_multiplier
                portfolio_value_mc += mc_weight * stock_value_multiplier
            else:
                # If stock doesn't have data for this date, use last known value or 1.0
                # Find the most recent return before this date
                last_return = 0.0
                if ticker in stock_returns_by_date:
                    for prev_date, prev_return in stock_returns_by_date[ticker].items():
                        if prev_date <= date:
                            last_return = prev_return
                stock_value_multiplier = 1.0 + (last_return / 100.0)
                portfolio_value_ebit += ebit_weight * stock_value_multiplier
                portfolio_value_mc += mc_weight * stock_value_multiplier
        
        # Calculate cumulative return percentages
        cumulative_return_ebit = (portfolio_value_ebit - 1.0) * 100
        cumulative_return_mc = (portfolio_value_mc - 1.0) * 100
        cumulative_returns_ebit_weighted.append(cumulative_return_ebit)
        cumulative_returns_market_cap.append(cumulative_return_mc)
    
    print(f"   Calculated returns for {len(sorted_dates)} time periods")
    print(f"   Start date: {sorted_dates[0].strftime('%Y-%m-%d')}")
    print(f"   End date: {sorted_dates[-1].strftime('%Y-%m-%d')}")
    print(f"   EBIT/PPE Weighted Total return: {cumulative_returns_ebit_weighted[-1]:.2f}%")
    print(f"   Market Cap Weighted Total return: {cumulative_returns_market_cap[-1]:.2f}%")
    
    # Create chart
    print("\n6. Creating performance chart...")
    plt.figure(figsize=(14, 8))
    
    # Plot both lines
    plt.plot(sorted_dates, cumulative_returns_ebit_weighted, linewidth=2, 
             color='#2E86AB', label='EBIT/PPE Weighted Portfolio')
    plt.plot(sorted_dates, cumulative_returns_market_cap, linewidth=2, 
             color='#A23B72', label='Market Cap Weighted Portfolio', linestyle='--')
    
    plt.title('Portfolio Performance Comparison (2002 - Present)\n'
              'EBIT/PPE Weighted vs Market Cap Weighted S&P 500 with Dividends Reinvested',
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
    plt.savefig('ebit_ppe_portfolio_backtest.png', dpi=300, bbox_inches='tight')
    print("   Chart saved to ebit_ppe_portfolio_backtest.png")
    plt.show()
    
    # Save results to JSON
    print("\n7. Saving results...")
    results = {
        'start_year': 2002,
        'total_stocks': len(stock_info),
        'start_date': sorted_dates[0].strftime('%Y-%m-%d'),
        'end_date': sorted_dates[-1].strftime('%Y-%m-%d'),
        'ebit_weighted_total_return_pct': cumulative_returns_ebit_weighted[-1],
        'market_cap_weighted_total_return_pct': cumulative_returns_market_cap[-1],
        'portfolio_weights': [
            {
                'ticker': s['ticker'],
                'ebit_ppe': s['ebit_ppe'],
                'market_cap': s['market_cap'],
                'initial_weight_pct': s['initial_weight'],
                'multiplier': s['multiplier'],
                'final_weight_pct': s['final_weight']
            }
            for s in stock_info
        ],
        'ebit_weighted_returns': [
            {
                'date': d.strftime('%Y-%m-%d'),
                'cumulative_return_pct': r
            }
            for d, r in zip(sorted_dates, cumulative_returns_ebit_weighted)
        ],
        'market_cap_weighted_returns': [
            {
                'date': d.strftime('%Y-%m-%d'),
                'cumulative_return_pct': r
            }
            for d, r in zip(sorted_dates, cumulative_returns_market_cap)
        ]
    }
    
    with open('ebit_ppe_portfolio_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("   Results saved to ebit_ppe_portfolio_results.json")
    print("\n" + "=" * 80)
    print("Backtest Complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()

