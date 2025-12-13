"""
Show statistics on individual stock returns from Glassdoor portfolios.

Reads the stock returns JSON files and displays various statistics.
"""
import json
import os
from typing import Dict, List
import numpy as np

# Get paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RETURNS_JSONS_DIR = os.path.join(SCRIPT_DIR, 'data', 'returns', 'jsons')


def load_all_stock_returns() -> Dict[int, List[Dict]]:
    """Load all stock returns data from JSON files."""
    all_returns = {}
    
    if not os.path.exists(RETURNS_JSONS_DIR):
        print(f"Returns directory not found: {RETURNS_JSONS_DIR}")
        return all_returns
    
    for filename in os.listdir(RETURNS_JSONS_DIR):
        if filename.startswith('glassdoor_') and filename.endswith('_stock_returns.json'):
            try:
                year = int(filename.split('_')[1])
                filepath = os.path.join(RETURNS_JSONS_DIR, filename)
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_returns[year] = data.get('stocks', [])
            except (ValueError, json.JSONDecodeError) as e:
                print(f"Error loading {filename}: {e}")
                continue
    
    return all_returns


def show_year_stats(year: int, stocks: List[Dict]):
    """Show statistics for a single year."""
    if not stocks:
        print(f"\n{year}: No stock data")
        return
    
    annualized_returns = [s['annualized_return_pct'] for s in stocks]
    total_returns = [s['total_return_pct'] for s in stocks]
    
    print(f"\n{'='*70}")
    print(f"  {year} - {len(stocks)} Stocks")
    print(f"{'='*70}")
    
    # Annualized return stats
    print(f"\n  Annualized Returns:")
    print(f"    Mean:   {np.mean(annualized_returns):>8.1f}%")
    print(f"    Median: {np.median(annualized_returns):>8.1f}%")
    print(f"    Std:    {np.std(annualized_returns):>8.1f}%")
    print(f"    Min:    {np.min(annualized_returns):>8.1f}%  ({stocks[-1]['ticker']} - {stocks[-1]['company']})")
    print(f"    Max:    {np.max(annualized_returns):>8.1f}%  ({stocks[0]['ticker']} - {stocks[0]['company']})")
    
    # Positive/negative
    positive = sum(1 for r in annualized_returns if r > 0)
    beat_10pct = sum(1 for r in annualized_returns if r > 10)
    beat_15pct = sum(1 for r in annualized_returns if r > 15)
    
    print(f"\n  Performance Distribution:")
    print(f"    Positive returns: {positive}/{len(stocks)} ({positive/len(stocks)*100:.0f}%)")
    print(f"    Beat 10%/year:    {beat_10pct}/{len(stocks)} ({beat_10pct/len(stocks)*100:.0f}%)")
    print(f"    Beat 15%/year:    {beat_15pct}/{len(stocks)} ({beat_15pct/len(stocks)*100:.0f}%)")
    
    # Top 5 performers
    print(f"\n  Top 5 Performers:")
    print(f"    {'Rank':<5} {'Ticker':<8} {'Company':<28} {'Ann. Ret':<12} {'Total Ret':<12}")
    print(f"    {'-'*5} {'-'*8} {'-'*28} {'-'*12} {'-'*12}")
    for i, stock in enumerate(stocks[:5]):
        company = stock['company'][:27] if stock['company'] else 'N/A'
        print(f"    {i+1:<5} {stock['ticker']:<8} {company:<28} {stock['annualized_return_pct']:>8.1f}%   {stock['total_return_pct']:>8.0f}%")
    
    # Bottom 5 performers
    print(f"\n  Bottom 5 Performers:")
    print(f"    {'Rank':<5} {'Ticker':<8} {'Company':<28} {'Ann. Ret':<12} {'Total Ret':<12}")
    print(f"    {'-'*5} {'-'*8} {'-'*28} {'-'*12} {'-'*12}")
    for i, stock in enumerate(stocks[-5:][::-1]):
        company = stock['company'][:27] if stock['company'] else 'N/A'
        print(f"    {len(stocks)-i:<5} {stock['ticker']:<8} {company:<28} {stock['annualized_return_pct']:>8.1f}%   {stock['total_return_pct']:>8.0f}%")


def show_aggregate_stats(all_returns: Dict[int, List[Dict]]):
    """Show aggregate statistics across all years."""
    all_stocks = []
    for year, stocks in all_returns.items():
        for stock in stocks:
            stock_copy = stock.copy()
            stock_copy['portfolio_year'] = year
            all_stocks.append(stock_copy)
    
    if not all_stocks:
        print("No stock data found")
        return
    
    annualized_returns = [s['annualized_return_pct'] for s in all_stocks]
    
    print(f"\n{'#'*70}")
    print(f"  AGGREGATE STATISTICS - ALL YEARS")
    print(f"{'#'*70}")
    
    print(f"\n  Total stock-years analyzed: {len(all_stocks)}")
    print(f"  Years covered: {min(all_returns.keys())} - {max(all_returns.keys())}")
    
    print(f"\n  Annualized Returns (All Stocks):")
    print(f"    Mean:   {np.mean(annualized_returns):>8.1f}%")
    print(f"    Median: {np.median(annualized_returns):>8.1f}%")
    print(f"    Std:    {np.std(annualized_returns):>8.1f}%")
    print(f"    Min:    {np.min(annualized_returns):>8.1f}%")
    print(f"    Max:    {np.max(annualized_returns):>8.1f}%")
    
    # Percentiles
    print(f"\n  Percentiles:")
    for p in [10, 25, 50, 75, 90]:
        print(f"    {p}th: {np.percentile(annualized_returns, p):>8.1f}%")
    
    # Performance distribution
    positive = sum(1 for r in annualized_returns if r > 0)
    beat_10pct = sum(1 for r in annualized_returns if r > 10)
    beat_15pct = sum(1 for r in annualized_returns if r > 15)
    beat_20pct = sum(1 for r in annualized_returns if r > 20)
    
    print(f"\n  Performance Distribution:")
    print(f"    Positive returns: {positive}/{len(all_stocks)} ({positive/len(all_stocks)*100:.1f}%)")
    print(f"    Beat 10%/year:    {beat_10pct}/{len(all_stocks)} ({beat_10pct/len(all_stocks)*100:.1f}%)")
    print(f"    Beat 15%/year:    {beat_15pct}/{len(all_stocks)} ({beat_15pct/len(all_stocks)*100:.1f}%)")
    print(f"    Beat 20%/year:    {beat_20pct}/{len(all_stocks)} ({beat_20pct/len(all_stocks)*100:.1f}%)")
    
    # Sort all stocks by annualized return
    all_stocks_sorted = sorted(all_stocks, key=lambda x: x['annualized_return_pct'], reverse=True)
    
    # Top 10 all-time
    print(f"\n  Top 10 All-Time Performers:")
    print(f"    {'Rank':<5} {'Year':<6} {'Ticker':<8} {'Company':<28} {'Ann. Ret':<10} {'Total':<10}")
    print(f"    {'-'*5} {'-'*6} {'-'*8} {'-'*28} {'-'*10} {'-'*10}")
    for i, stock in enumerate(all_stocks_sorted[:10]):
        company = stock['company'][:27] if stock['company'] else 'N/A'
        print(f"    {i+1:<5} {stock['portfolio_year']:<6} {stock['ticker']:<8} {company:<28} {stock['annualized_return_pct']:>7.1f}%  {stock['total_return_pct']:>8.0f}%")
    
    # Bottom 10 all-time
    print(f"\n  Bottom 10 All-Time Performers:")
    print(f"    {'Rank':<5} {'Year':<6} {'Ticker':<8} {'Company':<28} {'Ann. Ret':<10} {'Total':<10}")
    print(f"    {'-'*5} {'-'*6} {'-'*8} {'-'*28} {'-'*10} {'-'*10}")
    for i, stock in enumerate(all_stocks_sorted[-10:][::-1]):
        company = stock['company'][:27] if stock['company'] else 'N/A'
        rank = len(all_stocks_sorted) - i
        print(f"    {rank:<5} {stock['portfolio_year']:<6} {stock['ticker']:<8} {company:<28} {stock['annualized_return_pct']:>7.1f}%  {stock['total_return_pct']:>8.0f}%")
    
    # Most frequent companies
    company_counts = {}
    company_returns = {}
    for stock in all_stocks:
        company = stock['company']
        if company not in company_counts:
            company_counts[company] = 0
            company_returns[company] = []
        company_counts[company] += 1
        company_returns[company].append(stock['annualized_return_pct'])
    
    # Companies appearing in multiple years
    repeat_companies = [(c, cnt, np.mean(company_returns[c])) 
                        for c, cnt in company_counts.items() if cnt > 1]
    repeat_companies.sort(key=lambda x: x[1], reverse=True)
    
    if repeat_companies:
        print(f"\n  Most Frequent Companies (appeared multiple years):")
        print(f"    {'Company':<35} {'Count':<7} {'Avg Ann. Return':<15}")
        print(f"    {'-'*35} {'-'*7} {'-'*15}")
        for company, count, avg_ret in repeat_companies[:15]:
            company_display = company[:34] if company else 'N/A'
            print(f"    {company_display:<35} {count:<7} {avg_ret:>10.1f}%")


def main():
    """Main function."""
    print("Loading stock returns data...")
    all_returns = load_all_stock_returns()
    
    if not all_returns:
        print("No stock returns data found. Run calculate_glassdoor_returns.py first.")
        return
    
    print(f"Loaded data for {len(all_returns)} years: {sorted(all_returns.keys())}")
    
    # Show stats for each year
    for year in sorted(all_returns.keys()):
        show_year_stats(year, all_returns[year])
    
    # Show aggregate stats
    show_aggregate_stats(all_returns)


if __name__ == '__main__':
    main()

