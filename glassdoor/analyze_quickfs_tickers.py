"""
Analyze QuickFS ticker matching results to identify:
1. Companies that should have been matched but weren't
2. Companies that were incorrectly matched
3. Comparison with yfinance results
"""
import json
import os
from typing import Dict, List, Set

# Get paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
GLASSDOOR_DIR = SCRIPT_DIR
QUICKFS_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'tickers_quickfs')
YFINANCE_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'tickers_yfinance')
COMPANIES_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'companies')
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), 'data')


def load_ticker_data(year: int, source: str) -> Dict:
    """Load ticker data from QuickFS or yfinance."""
    if source == 'quickfs':
        file_path = os.path.join(QUICKFS_DIR, f'glassdoor_{year}_tickers.json')
    else:
        file_path = os.path.join(YFINANCE_DIR, f'glassdoor_{year}_tickers.json')
    
    if not os.path.exists(file_path):
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def check_ticker_in_data(ticker: str) -> Dict:
    """Check if a ticker exists in the data files and get company name."""
    for data_file in ['nyse_data.jsonl', 'nasdaq_data.jsonl']:
        file_path = os.path.join(DATA_DIR, data_file)
        if not os.path.exists(file_path):
            continue
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        stock = json.loads(line)
                        if stock.get('symbol', '').upper() == ticker.upper():
                            return {
                                'ticker': ticker,
                                'company_name': stock.get('company_name', 'N/A'),
                                'found': True
                            }
                    except:
                        continue
    
    return {'ticker': ticker, 'found': False}


def analyze_year(year: int):
    """Analyze ticker matching for a specific year."""
    print(f"\n{'='*80}")
    print(f"Analysis for Year {year}")
    print(f"{'='*80}")
    
    # Load data
    quickfs_data = load_ticker_data(year, 'quickfs')
    yfinance_data = load_ticker_data(year, 'yfinance')
    
    if not quickfs_data:
        print(f"No QuickFS data found for {year}")
        return
    
    # Load original company list
    companies_file = os.path.join(COMPANIES_DIR, f'glassdoor_{year}_companies.json')
    if os.path.exists(companies_file):
        with open(companies_file, 'r', encoding='utf-8') as f:
            all_companies = json.load(f)
    else:
        all_companies = []
    
    quickfs_matched = {m['glassdoor_name']: m['ticker'] for m in quickfs_data.get('matched', [])}
    quickfs_unmatched = set(quickfs_data.get('unmatched', []))
    
    if yfinance_data:
        yfinance_matched = {}
        if 'matched' in yfinance_data:
            for m in yfinance_data['matched']:
                name = m.get('glassdoor_name', '')
                ticker = m.get('ticker', '')
                if name and ticker:
                    yfinance_matched[name] = ticker
        yfinance_unmatched = set(yfinance_data.get('unmatched', []))
    else:
        yfinance_matched = {}
        yfinance_unmatched = set()
    
    print(f"\nQuickFS Results: {quickfs_data.get('stats', {}).get('matched', 0)} matched, {quickfs_data.get('stats', {}).get('unmatched', 0)} unmatched")
    if yfinance_data:
        print(f"YFinance Results: {len(yfinance_matched)} matched, {len(yfinance_unmatched)} unmatched")
    
    # 1. Check for potential false positives (matched by QuickFS but not by yfinance)
    print(f"\n{'='*80}")
    print("POTENTIAL FALSE POSITIVES (Matched by QuickFS but not by yfinance)")
    print(f"{'='*80}")
    false_positives = []
    for name, ticker in quickfs_matched.items():
        if name not in yfinance_matched:
            ticker_info = check_ticker_in_data(ticker)
            false_positives.append({
                'name': name,
                'ticker': ticker,
                'company_name_in_data': ticker_info.get('company_name', 'N/A') if ticker_info.get('found') else 'Not found in data',
                'found_in_data': ticker_info.get('found', False)
            })
    
    if false_positives:
        for fp in false_positives:
            print(f"\n  {fp['name']} -> {fp['ticker']}")
            print(f"    Company name in data: {fp['company_name_in_data']}")
            print(f"    Found in data files: {fp['found_in_data']}")
            if not fp['found_in_data']:
                print(f"    ⚠️  WARNING: Ticker not found in data files!")
    else:
        print("  None found")
    
    # 2. Check for missed matches (matched by yfinance but not by QuickFS)
    print(f"\n{'='*80}")
    print("POTENTIALLY MISSED MATCHES (Matched by yfinance but not by QuickFS)")
    print(f"{'='*80}")
    missed = []
    for name, ticker in yfinance_matched.items():
        if name not in quickfs_matched and name in quickfs_unmatched:
            ticker_info = check_ticker_in_data(ticker)
            missed.append({
                'name': name,
                'ticker': ticker,
                'company_name_in_data': ticker_info.get('company_name', 'N/A') if ticker_info.get('found') else 'Not found in data',
                'found_in_data': ticker_info.get('found', False)
            })
    
    if missed:
        for m in missed:
            print(f"\n  {m['name']} -> {m['ticker']} (yfinance found this)")
            print(f"    Company name in data: {m['company_name_in_data']}")
            print(f"    Found in data files: {m['found_in_data']}")
            if m['found_in_data']:
                print(f"    ⚠️  Should have been matched by QuickFS!")
    else:
        print("  None found")
    
    # 3. Check for ticker mismatches (same company, different ticker)
    print(f"\n{'='*80}")
    print("TICKER MISMATCHES (Same company matched to different tickers)")
    print(f"{'='*80}")
    mismatches = []
    for name in quickfs_matched.keys():
        if name in yfinance_matched:
            qf_ticker = quickfs_matched[name]
            yf_ticker = yfinance_matched[name]
            if qf_ticker != yf_ticker:
                qf_info = check_ticker_in_data(qf_ticker)
                yf_info = check_ticker_in_data(yf_ticker)
                mismatches.append({
                    'name': name,
                    'quickfs_ticker': qf_ticker,
                    'yfinance_ticker': yf_ticker,
                    'quickfs_company': qf_info.get('company_name', 'N/A') if qf_info.get('found') else 'Not found',
                    'yfinance_company': yf_info.get('company_name', 'N/A') if yf_info.get('found') else 'Not found'
                })
    
    if mismatches:
        for mm in mismatches:
            print(f"\n  {mm['name']}:")
            print(f"    QuickFS: {mm['quickfs_ticker']} ({mm['quickfs_company']})")
            print(f"    YFinance: {mm['yfinance_ticker']} ({mm['yfinance_company']})")
    else:
        print("  None found")
    
    # 4. Analyze unmatched companies that might be public
    print(f"\n{'='*80}")
    print("UNMATCHED COMPANIES ANALYSIS")
    print(f"{'='*80}")
    
    # Known private/non-profit companies (should remain unmatched)
    known_private = {
        'Bain & Company', 'Boston Consulting Group', 'McKinsey & Company',
        'Deloitte', 'EY', 'PwC', 'USAA', 'US Army', 'MITRE'
    }
    
    # Companies that might be public
    potentially_public = []
    for name in quickfs_unmatched:
        if name not in known_private:
            potentially_public.append(name)
    
    if potentially_public:
        print(f"\n  Potentially public companies that weren't matched ({len(potentially_public)}):")
        for name in sorted(potentially_public):
            print(f"    - {name}")
            # Check if yfinance found it
            if name in yfinance_matched:
                ticker = yfinance_matched[name]
                ticker_info = check_ticker_in_data(ticker)
                print(f"      → YFinance found: {ticker} ({ticker_info.get('company_name', 'N/A') if ticker_info.get('found') else 'Not in data'})")
                if ticker_info.get('found'):
                    print(f"      ⚠️  Should be matched by QuickFS!")
    
    # 5. Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"Total companies: {len(all_companies)}")
    print(f"QuickFS matched: {len(quickfs_matched)}")
    print(f"QuickFS unmatched: {len(quickfs_unmatched)}")
    if yfinance_data:
        print(f"YFinance matched: {len(yfinance_matched)}")
        print(f"YFinance unmatched: {len(yfinance_unmatched)}")
    print(f"\nPotential issues:")
    print(f"  - False positives: {len(false_positives)}")
    print(f"  - Missed matches: {len(missed)}")
    print(f"  - Ticker mismatches: {len(mismatches)}")
    print(f"  - Potentially public but unmatched: {len(potentially_public)}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze QuickFS ticker matching results')
    parser.add_argument('--year', type=int, default=2009, help='Year to analyze (default: 2009)')
    parser.add_argument('--all', action='store_true', help='Analyze all years')
    
    args = parser.parse_args()
    
    if args.all:
        years = list(range(2009, 2025))
    else:
        years = [args.year]
    
    for year in years:
        analyze_year(year)


if __name__ == '__main__':
    main()

