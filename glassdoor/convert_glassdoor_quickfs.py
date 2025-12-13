"""
Convert Glassdoor company names to tickers using QuickFS (NYSE/NASDAQ) data files.
Only includes companies that have price data for the specific year.
"""
import json
import os
import re
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime

# Get project root directory (1 level up from this script, since script is in glassdoor/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
GLASSDOOR_DIR = SCRIPT_DIR  # Script is already in glassdoor directory
COMPANIES_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'companies')
TICKERS_QUICKFS_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'tickers_quickfs')


def normalize_company_name_for_search(name: str) -> str:
    """Normalize company name for searching."""
    # Remove common suffixes and normalize
    name = name.strip()
    # Remove common suffixes
    suffixes = [' Inc', ' Inc.', ' Incorporated', ' Corp', ' Corp.', ' Corporation', 
                ' LLC', ' L.L.C.', ' Ltd', ' Ltd.', ' Limited', ' Company', ' Co', ' Co.',
                ' Technologies', ' Technology', ' Tech', ' Services', ' Service',
                ' Group', ' Holdings', ' Systems', ' System']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    return name


def get_company_name_variations(name: str) -> List[str]:
    """Generate variations of company name for matching."""
    variations = [name]
    normalized = normalize_company_name_for_search(name)
    if normalized != name:
        variations.append(normalized)
    
    # Try with/without "and"
    if '&' in name:
        variations.append(name.replace('&', 'and'))
        variations.append(name.replace('&', ''))
    if 'and' in name.lower():
        variations.append(name.replace('and', '&'))
    
    # Remove common prefixes
    prefixes = ['The ', 'A ', 'An ']
    for prefix in prefixes:
        if name.startswith(prefix):
            variations.append(name[len(prefix):])
    
    # Add common suffixes that might be in data but not in Glassdoor name
    common_suffixes = [' Inc', ' Inc.', ' Corporation', ' Corp', ' Corp.', ' Company', ' Co', ' Co.', 
                      ' LLC', ' Limited', ' Ltd', ' Ltd.']
    for suffix in common_suffixes:
        if not name.endswith(suffix):
            variations.append(name + suffix)
    
    return variations


def parse_date_string(date_str: str) -> Optional[int]:
    """Extract year from date string. Handles various formats."""
    if not date_str or date_str == '-':
        return None
    
    # Try YYYY-MM-DD format
    match = re.match(r'(\d{4})-\d{2}-\d{2}', str(date_str))
    if match:
        return int(match.group(1))
    
    # Try FY2009.FQ1 format
    match = re.match(r'FY(\d{4})\.FQ\d', str(date_str))
    if match:
        return int(match.group(1))
    
    # Try YYYY format
    match = re.match(r'(\d{4})', str(date_str))
    if match:
        return int(match.group(1))
    
    return None


def check_ticker_has_data_for_year(stock_data: Dict, year: int) -> Tuple[bool, Optional[str]]:
    """
    Check if a stock has price/market cap data for a specific year.
    
    Args:
        stock_data: Stock data dictionary from JSONL
        year: Target year to check
        
    Returns:
        Tuple of (has_data, info_message)
    """
    if not stock_data or "data" not in stock_data:
        return False, "No data field"
    
    data = stock_data.get("data", {})
    
    # Get period dates
    period_dates = None
    for date_key in ["period_end_date", "fiscal_quarter_key", "original_filing_date"]:
        if date_key in data and data[date_key]:
            period_dates = data[date_key]
            break
    
    if not period_dates or not isinstance(period_dates, list) or len(period_dates) == 0:
        return False, "No period dates found"
    
    # Check if we have market cap or price data for the target year
    market_caps = data.get("market_cap", [])
    prices = data.get("period_end_price", [])
    
    # Find any quarter in the target year
    for idx, date_str in enumerate(period_dates):
        date_year = parse_date_string(date_str)
        if date_year == year:
            # Check if we have data at this index
            has_market_cap = (isinstance(market_caps, list) and 
                            idx < len(market_caps) and 
                            market_caps[idx] is not None and 
                            market_caps[idx] > 0)
            has_price = (isinstance(prices, list) and 
                       idx < len(prices) and 
                       prices[idx] is not None and 
                       prices[idx] > 0)
            
            if has_market_cap or has_price:
                return True, f"Data found for {date_str}"
    
    # Also check nearby years (within 1 year)
    for idx, date_str in enumerate(period_dates):
        date_year = parse_date_string(date_str)
        if date_year and abs(date_year - year) <= 1:
            has_market_cap = (isinstance(market_caps, list) and 
                            idx < len(market_caps) and 
                            market_caps[idx] is not None and 
                            market_caps[idx] > 0)
            has_price = (isinstance(prices, list) and 
                       idx < len(prices) and 
                       prices[idx] is not None and 
                       prices[idx] > 0)
            
            if has_market_cap or has_price:
                return True, f"Data found for {date_str} (nearby year)"
    
    return False, f"No data found for year {year}"


def load_stock_data_by_ticker() -> Dict[str, Dict]:
    """
    Load all stock data from NYSE and NASDAQ files, indexed by ticker.
    
    Returns:
        Dict mapping ticker -> stock_data
    """
    stock_dict = {}
    
    # Load from NYSE data
    nyse_file = os.path.join(DATA_DIR, 'nyse_data.jsonl')
    if os.path.exists(nyse_file):
        print(f"Loading NYSE data from {nyse_file}...")
        try:
            with open(nyse_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            stock = json.loads(line)
                            ticker = stock.get('symbol', '').upper()
                            if ticker:
                                stock_dict[ticker] = stock
                        except json.JSONDecodeError:
                            continue
            print(f"  Loaded {len([t for t in stock_dict.keys() if t])} NYSE stocks")
        except Exception as e:
            print(f"Warning: Could not load NYSE data: {e}")
    
    # Load from NASDAQ data
    nasdaq_file = os.path.join(DATA_DIR, 'nasdaq_data.jsonl')
    if os.path.exists(nasdaq_file):
        print(f"Loading NASDAQ data from {nasdaq_file}...")
        try:
            with open(nasdaq_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            stock = json.loads(line)
                            ticker = stock.get('symbol', '').upper()
                            if ticker:
                                # NASDAQ may have duplicates, keep first one
                                if ticker not in stock_dict:
                                    stock_dict[ticker] = stock
                        except json.JSONDecodeError:
                            continue
            print(f"  Loaded {len([t for t in stock_dict.keys() if t])} total stocks (NYSE + NASDAQ)")
        except Exception as e:
            print(f"Warning: Could not load NASDAQ data: {e}")
    
    return stock_dict


def build_company_name_mapping_from_data(stock_dict: Dict[str, Dict]) -> Dict[str, str]:
    """
    Build a mapping of company names to tickers from stock data.
    
    Args:
        stock_dict: Dict mapping ticker -> stock_data
        
    Returns:
        Dict mapping company_name -> ticker
    """
    mapping = {}
    
    for ticker, stock in stock_dict.items():
        company_name = stock.get('company_name', '').strip()
        if company_name:
            # Store exact name
            mapping[company_name] = ticker
            # Store normalized version
            normalized = normalize_company_name_for_search(company_name)
            if normalized != company_name:
                mapping[normalized] = ticker
            # Store lowercase version for case-insensitive matching
            mapping[company_name.lower()] = ticker
            if normalized.lower() != company_name.lower():
                mapping[normalized.lower()] = ticker
    
    # Add known mappings for companies that might not match exactly
    # These are common variations that might appear in Glassdoor but not in data files
    known_mappings = {
        'Capital One': 'COF',
        'Capital One Financial': 'COF',
        'Capital One Financial Corporation': 'COF',
        'FactSet': 'FDS',
        'Factset': 'FDS',
        'Fact Set': 'FDS',
        'FactSet Research Systems': 'FDS',
        'FactSet Research Systems Inc': 'FDS',
        'Google': 'GOOGL',
        'Alphabet': 'GOOGL',
        'Alphabet Inc': 'GOOGL',
        'SAP': 'SAP',
        'Sap': 'SAP',
        'SAP SE': 'SAP',
        'Salesforce': 'CRM',
        'Salesforce.com': 'CRM',
        'Salesforce.com Inc': 'CRM',
        'Booz Allen Hamilton': 'BAH',
        'Booz Allen': 'BAH',
        'Booz Allen Hamilton Holding': 'BAH',
        'T Mobile': 'TMUS',
        'T-Mobile': 'TMUS',
        'T-Mobile US': 'TMUS',
        'Ellie Mae': 'ELLI',
        'Forrester': 'FORR',
        'Forrester Research': 'FORR',
        'Shell': 'SHEL',
        'Shell Oil': 'SHEL',
        'Royal Dutch Shell': 'SHEL',
        'Genentech': 'DNA',
        'Genentech Inc': 'DNA',
        'Wells Fargo': 'WFC',
        'Wells Fargo & Company': 'WFC',
        'Wells Fargo Company': 'WFC',
        'Accenture': 'ACN',
        'Accenture plc': 'ACN',
        'Schlumberger': 'SLB',
        'Schlumberger Limited': 'SLB',
        'Schlumberger Ltd': 'SLB',
        'Continental Airlines': 'CAL',
        'Continental': 'CAL',
        'Continental Airlines Inc': 'CAL',
    }
    
    # Add known mappings to the main mapping
    for name, ticker in known_mappings.items():
        if ticker in stock_dict:  # Only add if ticker exists in our data
            mapping[name] = ticker
            mapping[name.lower()] = ticker
            normalized = normalize_company_name_for_search(name)
            if normalized != name:
                mapping[normalized] = ticker
                mapping[normalized.lower()] = ticker
    
    return mapping


def find_ticker_for_company(company_name: str, data_mapping: Dict[str, str], 
                           stock_dict: Dict[str, Dict], year: int) -> Optional[Tuple[str, str]]:
    """
    Find ticker for a company and verify it has data for the year.
    
    Args:
        company_name: Glassdoor company name
        data_mapping: Mapping of company names to tickers
        stock_dict: Dict mapping ticker -> stock_data
        year: Target year to verify data exists
        
    Returns:
        Tuple of (ticker, info_message) or None
    """
    # Known false positives to exclude
    false_positives = {
        'Northwestern Mutual': ['NWE'],  # NWE is NorthWestern Corporation, not Northwestern Mutual
        'T Mobile': ['SCKT'],  # SCKT is Socket Mobile Inc, not T-Mobile
        'T-Mobile': ['SCKT'],  # SCKT is Socket Mobile Inc, not T-Mobile
        'Liberty National': ['NHLD'],  # NHLD is National Holdings Corporation
        'Liberty Mutual': ['NHLD'],  # NHLD is National Holdings Corporation
    }
    
    excluded_tickers = false_positives.get(company_name, [])
    
    # Try exact match
    if company_name in data_mapping:
        ticker = data_mapping[company_name]
        if ticker not in excluded_tickers and ticker in stock_dict:
            has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
            if has_data:
                return (ticker, info)
    
    # Try normalized version
    normalized = normalize_company_name_for_search(company_name)
    if normalized in data_mapping:
        ticker = data_mapping[normalized]
        if ticker not in excluded_tickers and ticker in stock_dict:
            has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
            if has_data:
                return (ticker, info)
    
    # Try case-insensitive lookup
    company_lower = company_name.lower()
    if company_lower in data_mapping:
        ticker = data_mapping[company_lower]
        if ticker not in excluded_tickers and ticker in stock_dict:
            has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
            if has_data:
                return (ticker, info)
    
    # Try variations
    variations = get_company_name_variations(company_name)
    for variation in variations:
        if variation in data_mapping:
            ticker = data_mapping[variation]
            if ticker not in excluded_tickers and ticker in stock_dict:
                has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
                if has_data:
                    return (ticker, info)
        # Also try lowercase
        if variation.lower() in data_mapping:
            ticker = data_mapping[variation.lower()]
            if ticker not in excluded_tickers and ticker in stock_dict:
                has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
                if has_data:
                    return (ticker, info)
    
    # Try fuzzy matching - check if any key in data_mapping contains the company name or vice versa
    company_words = set(company_name.lower().split())
    company_lower = company_name.lower()
    
    # First, try substring matching (one contains the other)
    for key, ticker in data_mapping.items():
        if ticker in excluded_tickers:
            continue
        key_lower = key.lower()
        # Check if one name is contained in the other (for cases like "Capital One" vs "Capital One Financial")
        if (company_lower in key_lower or key_lower in company_lower) and len(company_name) >= 5 and len(key) >= 5:
            if ticker in stock_dict:
                has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
                if has_data:
                    return (ticker, info)
    
    # Then try word overlap matching
    for key, ticker in data_mapping.items():
        if ticker in excluded_tickers:
            continue
        key_words = set(key.lower().split())
        # If there's significant overlap in words (at least 2 words match)
        if len(company_words) >= 2 and len(key_words) >= 2:
            common_words = company_words.intersection(key_words)
            # Require at least 2 common words, or all words from the shorter name match
            min_words = min(len(company_words), len(key_words))
            if len(common_words) >= 2 and len(common_words) >= min_words * 0.7:  # At least 70% of shorter name matches
                if ticker in stock_dict:
                    has_data, info = check_ticker_has_data_for_year(stock_dict[ticker], year)
                    if has_data:
                        return (ticker, info)
    
    return None


def process_single_company(company_name: str, year: int, data_mapping: Dict[str, str],
                           stock_dict: Dict[str, Dict], index: int, total: int) -> Tuple[Optional[str], Optional[str], str]:
    """Process a single company to find its ticker."""
    result = find_ticker_for_company(company_name, data_mapping, stock_dict, year)
    
    if result:
        ticker, info = result
        return (ticker, info, company_name)
    else:
        return (None, None, company_name)


def convert_glassdoor_year_from_data(year: int, stock_dict: Dict[str, Dict] = None, 
                                     data_mapping: Dict[str, str] = None,
                                     max_workers: int = 10, use_cache: bool = True) -> Dict:
    """
    Convert Glassdoor company names to tickers for a specific year using data files.
    Only includes companies that have price data for that year.
    
    Args:
        year: Year of the Glassdoor list
        stock_dict: Pre-loaded stock data dict (ticker -> stock_data). If None, will load.
        data_mapping: Pre-built company name mapping. If None, will build from stock_dict.
        max_workers: Number of threads for processing
        use_cache: Whether to use cached results
        
    Returns:
        Dict with matched and unmatched companies
    """
    # Ensure output directory exists
    os.makedirs(TICKERS_QUICKFS_DIR, exist_ok=True)
    
    # Load cached results if available
    cached_results = None
    cache_file = os.path.join(TICKERS_QUICKFS_DIR, f'glassdoor_{year}_tickers.json')
    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_results = json.load(f)
            print(f"Loaded cached results for {year}: {cached_results.get('stats', {}).get('matched', 0)} matched, {cached_results.get('stats', {}).get('unmatched', 0)} unmatched")
        except Exception as e:
            print(f"Warning: Could not load cache: {e}")
    
    # Load Glassdoor companies for the year
    glassdoor_file = os.path.join(COMPANIES_DIR, f'glassdoor_{year}_companies.json')
    if not os.path.exists(glassdoor_file):
        print(f"Error: Glassdoor companies file not found: {glassdoor_file}")
        return cached_results or {"matched": [], "unmatched": [], "stats": {"total": 0, "matched": 0, "unmatched": 0, "match_rate": 0.0}}
    
    with open(glassdoor_file, 'r', encoding='utf-8') as f:
        glassdoor_companies = json.load(f)
    
    if not isinstance(glassdoor_companies, list):
        print(f"Error: Invalid format in {glassdoor_file}")
        return cached_results or {"matched": [], "unmatched": [], "stats": {"total": 0, "matched": 0, "unmatched": 0, "match_rate": 0.0}}
    
    print(f"\nLoaded {len(glassdoor_companies)} companies from Glassdoor {year} list")
    
    # Load stock data if not provided
    if stock_dict is None:
        print("\nLoading stock data from NYSE and NASDAQ files...")
        stock_dict = load_stock_data_by_ticker()
        
        if not stock_dict:
            print("Error: No stock data loaded. Please check that nyse_data.jsonl and nasdaq_data.jsonl exist.")
            return cached_results or {"matched": [], "unmatched": [], "stats": {"total": len(glassdoor_companies), "matched": 0, "unmatched": len(glassdoor_companies), "match_rate": 0.0}}
    
    # Build company name mapping if not provided
    if data_mapping is None:
        print("Building company name to ticker mapping...")
        data_mapping = build_company_name_mapping_from_data(stock_dict)
        print(f"Built mapping for {len(data_mapping)} company name variations")
    
    # Determine which companies to process
    companies_to_process = []
    cached_matched = {}
    cached_unmatched = set()
    
    if cached_results:
        for match in cached_results.get('matched', []):
            cached_matched[match.get('glassdoor_name', '')] = match
        for unmatched_name in cached_results.get('unmatched', []):
            cached_unmatched.add(unmatched_name)
        companies_to_process = [c for c in glassdoor_companies if c in cached_unmatched]
        print(f"\nUsing cache: {len(cached_matched)} already matched, {len(cached_unmatched)} to re-process")
        print(f"Processing {len(companies_to_process)} previously unmatched companies using {max_workers} threads...")
    else:
        companies_to_process = glassdoor_companies
        print(f"\nProcessing {len(glassdoor_companies)} companies for year {year} using {max_workers} threads...")
    
    if not companies_to_process and cached_results:
        print("All companies already processed. Using cached results.")
        return cached_results
    
    matched = []
    unmatched = []
    results_lock = threading.Lock()
    completed_count = 0
    total_count = len(companies_to_process)
    
    if companies_to_process:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_company = {
                executor.submit(process_single_company, company_name, year, data_mapping, stock_dict, i, total_count): company_name
                for i, company_name in enumerate(companies_to_process, 1)
            }
            
            for future in as_completed(future_to_company):
                ticker, info, company_name = future.result()
                
                with results_lock:
                    completed_count += 1
                    if ticker:
                        matched.append({
                            "glassdoor_name": company_name,
                            "ticker": ticker,
                            "info": info,
                            "year": year
                        })
                        status = "[OK]"
                        print(f"[{completed_count}/{total_count}] {status} {company_name} -> {ticker} ({info})")
                    else:
                        unmatched.append(company_name)
                        status = "[NO]"
                        print(f"[{completed_count}/{total_count}] {status} {company_name} (No match or no data for {year})")
    
    # Merge with cached results
    if cached_results:
        # Add cached matched
        for match in cached_results.get('matched', []):
            if match.get('glassdoor_name') not in [m.get('glassdoor_name') for m in matched]:
                matched.append(match)
        # Update unmatched list (remove newly matched, keep others)
        final_unmatched = [u for u in cached_results.get('unmatched', []) if u not in [m.get('glassdoor_name') for m in matched]]
        final_unmatched.extend([u for u in unmatched if u not in final_unmatched])
        unmatched = final_unmatched
    
    # Sort matched by glassdoor_name
    matched.sort(key=lambda x: x.get('glassdoor_name', ''))
    unmatched.sort()
    
    # Calculate statistics
    total = len(glassdoor_companies)
    matched_count = len(matched)
    unmatched_count = len(unmatched)
    match_rate = (matched_count / total * 100) if total > 0 else 0.0
    
    stats = {
        "total": total,
        "matched": matched_count,
        "unmatched": unmatched_count,
        "match_rate": round(match_rate, 2)
    }
    
    result = {
        "year": year,
        "matched": matched,
        "unmatched": unmatched,
        "stats": stats
    }
    
    # Save results
    os.makedirs(TICKERS_QUICKFS_DIR, exist_ok=True)
    output_file = os.path.join(TICKERS_QUICKFS_DIR, f'glassdoor_{year}_tickers.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\n{'='*60}")
    print(f"Results for {year}:")
    print(f"  Total companies: {total}")
    print(f"  Matched: {matched_count} ({match_rate:.1f}%)")
    print(f"  Unmatched: {unmatched_count}")
    print(f"  Saved to: {output_file}")
    print(f"{'='*60}")
    
    return result


def main():
    """Main function to run the converter."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert Glassdoor company names to tickers using QuickFS data files')
    parser.add_argument('--year', type=int, help='Year to convert (2009-2025)')
    parser.add_argument('--all', action='store_true', help='Process all years')
    parser.add_argument('--workers', type=int, default=10, help='Number of worker threads (default: 10)')
    parser.add_argument('--no-cache', action='store_true', help='Disable caching')
    
    args = parser.parse_args()
    
    current_year = datetime.now().year
    max_year = current_year + 1
    
    if args.all:
        years = list(range(2009, max_year + 1))
    elif args.year:
        if 2009 <= args.year <= max_year:
            years = [args.year]
        else:
            print(f"Error: Year must be between 2009 and {max_year}")
            return
    else:
        years = None  # Will be set in interactive mode
        # Interactive mode
        print("Glassdoor Company Name to Ticker Converter (Using Data Files)")
        print("=" * 60)
        print(f"Enter a year (2009-{max_year}) or 'all' for all years")
        print("=" * 60)
        
        while True:
            try:
                year_input = input(f"\nEnter the year to convert (2009-{max_year}) or 'all' for all years (or 'quit'/'exit' to stop): ").strip().lower()
                
                if year_input in ['quit', 'exit', 'q']:
                    print("\nExiting converter. Goodbye!")
                    break
                
                if year_input == 'all':
                    years = list(range(2009, max_year + 1))
                    break
                else:
                    year = int(year_input)
                    if 2009 <= year <= max_year:
                        years = [year]
                        break
                    else:
                        print(f"Error: Year must be between 2009 and {max_year}. Please try again.")
            except ValueError:
                print(f"Error: '{year_input}' is not a valid year. Please enter a number between 2009 and {max_year}, 'all', or 'quit'.")
            except KeyboardInterrupt:
                print("\n\nConverter cancelled by user. Exiting...")
                return
    
    # Load stock data once for all years (if processing multiple years)
    if len(years) > 1:
        print("\n" + "="*60)
        print("Loading stock data from NYSE and NASDAQ files (one-time load)...")
        print("="*60)
        stock_dict = load_stock_data_by_ticker()
        
        if not stock_dict:
            print("Error: No stock data loaded. Please check that nyse_data.jsonl and nasdaq_data.jsonl exist.")
            return
        
        # Build company name mapping once for all years
        print("\nBuilding company name to ticker mapping (one-time build)...")
        data_mapping = build_company_name_mapping_from_data(stock_dict)
        print(f"Built mapping for {len(data_mapping)} company name variations")
        print("\n" + "="*60)
        print("Data loaded. Processing years...")
        print("="*60)
    else:
        # Single year - will load inside function
        stock_dict = None
        data_mapping = None
    
    # Process each year
    for year in years:
        try:
            convert_glassdoor_year_from_data(year, stock_dict=stock_dict, data_mapping=data_mapping,
                                           max_workers=args.workers, use_cache=not args.no_cache)
            if len(years) > 1 and year != years[-1]:
                print("\nWaiting 2 seconds before next year...")
                import time
                time.sleep(2)
        except Exception as e:
            print(f"\nError processing {year}: {e}")
            import traceback
            traceback.print_exc()
    
    if len(years) > 1:
        print(f"\n{'='*60}")
        print(f"Completed processing {len(years)} years!")
        print(f"{'='*60}")


if __name__ == '__main__':
    main()

