"""
Convert Glassdoor company names to stock tickers using yfinance.
Checks if companies were public at the time of the list.
"""
import json
import os
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import yfinance as yf

# Get project root directory (2 levels up from this script)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
GLASSDOOR_DIR = os.path.join(DATA_DIR, 'glassdoor')


def normalize_company_name_for_search(name: str) -> str:
    """Normalize company name for searching."""
    # Remove common suffixes and normalize
    name = name.strip()
    # Remove common suffixes
    suffixes = [' Inc', ' Inc.', ' Incorporated', ' Corp', ' Corp.', ' Corporation', 
                ' LLC', ' L.L.C.', ' Ltd', ' Ltd.', ' Limited', ' Company', ' Co', ' Co.']
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
    return name


def get_company_name_variations(name: str) -> List[str]:
    """Generate possible variations of a company name for searching."""
    variations = [name]
    
    # Remove common words
    common_words = ['The', 'A', 'An']
    words = name.split()
    if words and words[0] in common_words:
        variations.append(' '.join(words[1:]))
    
    # Try without "&" variations
    if '&' in name:
        variations.append(name.replace('&', 'and'))
        variations.append(name.replace('&', ''))
    if 'and' in name.lower():
        variations.append(name.replace('and', '&'))
    
    # Try acronym versions for known companies
    known_acronyms = {
        'International Business Machines': 'IBM',
        'International Business Machines Corporation': 'IBM',
        'Hewlett Packard': 'HP',
        'Hewlett-Packard': 'HP',
        'General Electric': 'GE',
        'AT&T': 'T',
        'American Telephone and Telegraph': 'T',
    }
    for full_name, ticker in known_acronyms.items():
        if full_name.lower() in name.lower():
            variations.append(ticker)
    
    return variations


def build_company_name_mapping_from_data() -> Dict[str, str]:
    """
    Build a mapping of company names to tickers from existing stock data files.
    
    Returns:
        Dict mapping company_name -> ticker
    """
    mapping = {}
    
    # Load from NYSE data
    nyse_file = os.path.join(DATA_DIR, 'nyse_data.jsonl')
    if os.path.exists(nyse_file):
        try:
            with open(nyse_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            stock = json.loads(line)
                            ticker = stock.get('symbol', '').upper()
                            company_name = stock.get('company_name', '').strip()
                            if ticker and company_name:
                                # Store both exact and normalized versions
                                mapping[company_name] = ticker
                                normalized = normalize_company_name_for_search(company_name)
                                if normalized != company_name:
                                    mapping[normalized] = ticker
                        except:
                            continue
        except Exception as e:
            print(f"Warning: Could not load NYSE data: {e}")
    
    # Load from NASDAQ data
    nasdaq_file = os.path.join(DATA_DIR, 'nasdaq_data.jsonl')
    if os.path.exists(nasdaq_file):
        try:
            with open(nasdaq_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            stock = json.loads(line)
                            ticker = stock.get('symbol', '').upper()
                            company_name = stock.get('company_name', '').strip()
                            if ticker and company_name:
                                # Store both exact and normalized versions
                                mapping[company_name] = ticker
                                normalized = normalize_company_name_for_search(company_name)
                                if normalized != company_name:
                                    mapping[normalized] = ticker
                        except:
                            continue
        except Exception as e:
            print(f"Warning: Could not load NASDAQ data: {e}")
    
    return mapping


def try_ticker_from_name(company_name: str, data_mapping: Dict[str, str] = None) -> Optional[str]:
    """Try to guess ticker from company name using data mapping and common patterns."""
    if data_mapping is None:
        data_mapping = {}
    
    # First, try data mapping (most reliable)
    if company_name in data_mapping:
        return data_mapping[company_name]
    
    # Try normalized version
    normalized = normalize_company_name_for_search(company_name)
    if normalized in data_mapping:
        return data_mapping[normalized]
    
    # Try case-insensitive lookup in data mapping
    for key, value in data_mapping.items():
        if key.lower() == company_name.lower():
            return value
    
    # Known company name to ticker mappings (fallback for companies not in data files)
    # Note: Some companies changed tickers (e.g., Facebook -> Meta)
    known_mappings = {
        'Facebook': 'META',  # Facebook changed to Meta (was FB before 2022)
        'Google': 'GOOGL',
        'Alphabet': 'GOOGL',
        'Microsoft': 'MSFT',
        'Apple': 'AAPL',
        'Amazon': 'AMZN',
        'Nvidia': 'NVDA',
        'Tesla': 'TSLA',
        'Netflix': 'NFLX',
        'Salesforce': 'CRM',
        'Adobe': 'ADBE',
        'Intuit': 'INTU',
        'DocuSign': 'DOCU',
        'HubSpot': 'HUBS',
        'LinkedIn': 'MSFT',  # Acquired by Microsoft
        'Linkedin': 'MSFT',
        'VMware': 'VMW',
        'Vmware': 'VMW',
        'SAP': 'SAP',
        'Nike': 'NKE',
        'NIKE': 'NKE',
        'Starbucks': 'SBUX',
        'Johnson & Johnson': 'JNJ',
        'Procter & Gamble': 'PG',
        '3M': 'MMM',
        '3m': 'MMM',
        'Cisco Systems': 'CSCO',
        'Cisco': 'CSCO',
        'Delta Air Lines': 'DAL',
        'Southwest Airlines': 'LUV',
        'United Airlines': 'UAL',
        'Hilton': 'HLT',
        'Hyatt': 'H',
        'Zillow': 'Z',
        'Electronic Arts': 'EA',
        'Stryker': 'SYK',
        'Boston Scientific': 'BSX',
        'Eli Lilly & Company': 'LLY',
        'Eli Lilly': 'LLY',
        'Capital One': 'COF',
        'Travelers': 'TRV',
        'Shell': 'SHEL',
        'Walt Disney Company': 'DIS',
        'Disney': 'DIS',
        'Yahoo': 'AABA',  # Now part of Verizon
        'Rei': None,  # Private
        'Trader Joe\'s': None,  # Private
        'Trader Joe S': None,  # Private
        'Chick-fil-A': None,  # Private
        'Chick Fil A': None,  # Private
        'In-N-Out Burger': None,  # Private
        'In N Out Burger': None,  # Private
        'Bain & Company': None,  # Private
        'Boston Consulting Group': None,  # Private
        'McKinsey & Company': None,  # Private
        'Deloitte': None,  # Private
        'KPMG': None,  # Private
        'Kpmg': None,  # Private
        'PwC': None,  # Private
        'EY': None,  # Private
        'Accenture': 'ACN',
        'CDW': 'CDW',
        'Cdw': 'CDW',
        'Paylocity': 'PCTY',
        'Ceridian': 'CDAY',
        'Insperity': 'NSP',
        'Ultimate Software': 'ULTI',  # Now UKG after merger
        'Taylor Morrison': 'TMHC',
        'Avalonbay Communities': 'AVB',
        'AvalonBay Communities': 'AVB',
        'Extra Space Storage': 'EXR',
        'Oshkosh Corporation': 'OSK',
        'T-Mobile': 'TMUS',
        'T Mobile': 'TMUS',
        'Lululemon': 'LULU',
        'Adidas': 'ADDYY',  # ADR
        'Roche': 'RHHBY',  # ADR
        'Nestlé': 'NSRGY',  # ADR
        'Nestlé Purina': 'NSRGY',  # ADR
        'Nestléé Purina': 'NSRGY',  # ADR
        # Additional companies from Glassdoor lists
        'General Mills': 'GIS',
        'Whole Foods Market': 'WFM',  # Acquired by Amazon
        'NetApp': 'NTAP',
        'Continental Airlines': 'CAL',  # Merged with United
        'FactSet': 'FDS',
        'Caterpillar': 'CAT',
        'Genentech': 'DNA',  # Acquired by Roche
        'Juniper Networks': 'JNPR',
        'Marriott International': 'MAR',
        'Chevron': 'CVX',
        'Goldman Sachs': 'GS',
        'Nordstrom': 'JWN',
        'Citrix': 'CTXS',  # Acquired by Cloud Software Group
        'Schlumberger': 'SLB',
        'National Instruments': 'NATI',  # Acquired by Emerson
        'Novell': None,  # Acquired
        'American Express': 'AXP',
        'Qualcomm': 'QCOM',
        'Lockheed Martin': 'LMT',
        'Texas Instruments': 'TXN',
        'Wells Fargo': 'WFC',
        'Best Buy': 'BBY',
        'Paychex': 'PAYX',
        'World Wide Technology': None,  # Private
        'St Jude Children S Research Hospital': None,  # Non-profit
        'Keller Williams': None,  # Private
        'E & J Gallo Winery': None,  # Private
        'Power Home Remodeling': None,  # Private
        'Academy Mortgage': None,  # Private
        'The Church Of Jesus Christ Of Latter Day Saints': None,  # Non-profit
        'H E B': None,  # Private
        'Fast Enterprises': None,  # Private
        'Blizzard Entertainment': 'ATVI',  # Now part of Microsoft
        'Newyork Presbyterian Hospital': None,  # Non-profit
        'SAP Concur': 'SAP',  # Subsidiary
        'Forrester': 'FORR',
        'Kimpton Hotels & Restaurants': 'IHG',  # Part of IHG
        'Ellie Mae': 'ELLI',  # Acquired by Intercontinental Exchange
        'Yardi Systems': None,  # Private
        'Smile Brands': None,  # Private
        'Progressive Leasing': 'PRG',  # Subsidiary of Progressive
        'Memorial Sloan Kettering': None,  # Non-profit
        'Texas Health Resources': None,  # Non-profit
        'Protiviti': None,  # Private
        'Wegmans Food Markets': None,  # Private
        'SpaceX': None,  # Private
        'Spacex': None,  # Private
        'Discount Tire': None,  # Private
        'Discount TIre': None,  # Private (typo in original)
        'Rei': None,  # Private co-op
        'Kwik Trip': None,  # Private
        'Arm': 'ARM',
        'Northwestern Mutual': None,  # Mutual company
        'Guidewire': 'GWRE',
        'Trader Joe S': None,  # Private
        'Slalom': None,  # Private
        'J Crew': None,  # Private (bankrupt, now private)
        'Toyota North America': 'TM',  # ADR
        'Aurora Health Care': None,  # Non-profit
        'Darden': 'DRI',
        'Quiktrip': None,  # Private
        'Massachusetts General Hospital': None,  # Non-profit
        'Kaiser Permanente': None,  # Non-profit
        'Morrison Healthcare': None,  # Private
        'Liberty National': None,  # Insurance, private
        'Bayada Home Health Care': None,  # Private
    }
    
    # Direct lookup
    if company_name in known_mappings:
        return known_mappings[company_name]
    
    # Try normalized lookup
    normalized = normalize_company_name_for_search(company_name)
    if normalized in known_mappings:
        return known_mappings[normalized]
    
    # Try case-insensitive lookup
    for key, value in known_mappings.items():
        if key.lower() == company_name.lower():
            return value
    
    return None


def get_ticker_info(ticker: str) -> Optional[Dict]:
    """Get company info from yfinance for a ticker."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Check if we got valid info
        if not info or 'symbol' not in info:
            return None
        
        return info
    except Exception as e:
        return None


def check_company_public_at_year(ticker: str, year: int) -> Tuple[bool, Optional[str]]:
    """
    Check if a company was public at a given year.
    Handles ticker changes (e.g., Facebook/FB -> Meta/META).
    
    Returns:
        (is_public, ipo_date_or_error_message)
    """
    # Handle historical ticker changes
    historical_tickers = {
        'META': ('FB', 2012),  # Facebook was FB until 2022, then changed to META
    }
    
    # Check if we need to use historical ticker
    if ticker in historical_tickers:
        old_ticker, change_year = historical_tickers[ticker]
        if year < change_year:
            ticker = old_ticker
    
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Try to get historical data first (more reliable for old years)
        try:
            # Get data up to the year we're checking
            end_date = f"{year}-12-31"
            hist = stock.history(start=f"{year}-01-01", end=end_date)
            
            if hist is not None and len(hist) > 0:
                # Company was trading in this year, so it was public
                # Get first available date for IPO info
                full_hist = stock.history(period="max")
                if full_hist is not None and len(full_hist) > 0:
                    first_date = full_hist.index[0]
                    ipo_year = first_date.year
                    if ipo_year <= year:
                        return True, f"First trade: {first_date.strftime('%Y-%m-%d')}"
        except:
            pass
        
        if not info:
            return False, "No info available"
        
        # Get IPO date from info
        ipo_date = info.get('ipoDate')
        if not ipo_date:
            # Try to get from first available data
            try:
                hist = stock.history(period="max")
                if hist is not None and len(hist) > 0:
                    first_date = hist.index[0]
                    ipo_year = first_date.year
                    if ipo_year <= year:
                        return True, f"First trade: {first_date.strftime('%Y-%m-%d')}"
                    else:
                        return False, f"First trade: {first_date.strftime('%Y-%m-%d')} (after {year})"
            except:
                pass
            
            return False, "IPO date unknown"
        
        # Parse IPO date
        if isinstance(ipo_date, (int, float)):
            # Sometimes IPO date is a timestamp
            try:
                ipo_datetime = datetime.fromtimestamp(ipo_date / 1000)  # Assume milliseconds
                ipo_year = ipo_datetime.year
            except:
                return False, f"Could not parse IPO timestamp: {ipo_date}"
        elif isinstance(ipo_date, str):
            try:
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z']:
                    try:
                        ipo_datetime = datetime.strptime(ipo_date, fmt)
                        ipo_year = ipo_datetime.year
                        break
                    except:
                        continue
                else:
                    return False, f"Could not parse IPO date: {ipo_date}"
            except:
                return False, f"Could not parse IPO date: {ipo_date}"
        else:
            return False, f"Unexpected IPO date format: {type(ipo_date)}"
        
        # Check if IPO was before or during the year
        if ipo_year <= year:
            return True, str(ipo_date)
        else:
            return False, f"IPO: {ipo_date} (after {year})"
            
    except Exception as e:
        return False, f"Error: {str(e)}"


def find_ticker_for_company(company_name: str, year: int, data_mapping: Dict[str, str] = None) -> Optional[Dict]:
    """
    Find ticker for a company name, checking if it was public at the given year.
    
    Returns:
        Dict with 'ticker', 'company_name', 'public_at_year', 'ipo_date', 'match_method'
        or None if not found/not public
    """
    # First, try data mapping and known mappings
    ticker = try_ticker_from_name(company_name, data_mapping)
    
    if ticker is None:
        # Try variations
        variations = get_company_name_variations(company_name)
        for variation in variations:
            ticker = try_ticker_from_name(variation, data_mapping)
            if ticker:
                break
    
    if ticker is None:
        return None
    
    # Check if company was public at that year
    is_public, ipo_info = check_company_public_at_year(ticker, year)
    
    if is_public:
        # Get company info to verify name match
        info = get_ticker_info(ticker)
        if info:
            official_name = info.get('longName') or info.get('shortName', '') or ticker
            match_method = 'data_mapping' if company_name.lower() in [k.lower() for k in (data_mapping or {}).keys()] else 'known_mapping'
            return {
                'ticker': ticker,
                'company_name': official_name,
                'glassdoor_name': company_name,
                'public_at_year': True,
                'ipo_date': ipo_info,
                'match_method': match_method
            }
        else:
            # Even if we can't get full info, if IPO check passed, return the match
            match_method = 'data_mapping' if company_name.lower() in [k.lower() for k in (data_mapping or {}).keys()] else 'known_mapping'
            return {
                'ticker': ticker,
                'company_name': company_name,  # Use Glassdoor name as fallback
                'glassdoor_name': company_name,
                'public_at_year': True,
                'ipo_date': ipo_info,
                'match_method': match_method
            }
    
    return None


def convert_glassdoor_year_to_tickers(year: int) -> Dict:
    """
    Convert Glassdoor company names to tickers for a specific year.
    
    Returns:
        Dict with 'year', 'companies', 'matched', 'unmatched', 'stats'
    """
    # Build company name mapping from existing data files
    print("Building company name mapping from existing stock data...")
    data_mapping = build_company_name_mapping_from_data()
    print(f"Loaded {len(data_mapping)} company name mappings from data files")
    
    # Load Glassdoor companies for the year
    glassdoor_file = os.path.join(GLASSDOOR_DIR, f'glassdoor_{year}_companies.json')
    
    if not os.path.exists(glassdoor_file):
        print(f"Error: {glassdoor_file} not found")
        return None
    
    with open(glassdoor_file, 'r', encoding='utf-8') as f:
        glassdoor_companies = json.load(f)
    
    print(f"\nProcessing {len(glassdoor_companies)} companies for year {year}...")
    
    matched = []
    unmatched = []
    
    for i, company_name in enumerate(glassdoor_companies, 1):
        print(f"[{i}/{len(glassdoor_companies)}] Processing: {company_name}")
        
        result = find_ticker_for_company(company_name, year, data_mapping)
        
        if result:
            matched.append(result)
            print(f"  ✓ Matched: {company_name} -> {result['ticker']} (IPO: {result['ipo_date']})")
        else:
            unmatched.append(company_name)
            print(f"  ✗ No match or not public: {company_name}")
        
        # Be polite to yfinance API
        time.sleep(0.5)
    
    return {
        'year': year,
        'companies': glassdoor_companies,
        'matched': matched,
        'unmatched': unmatched,
        'stats': {
            'total': len(glassdoor_companies),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'match_rate': len(matched) / len(glassdoor_companies) * 100 if glassdoor_companies else 0
        }
    }


def save_ticker_mapping(results: Dict, year: int):
    """Save ticker mapping results to JSON file."""
    output_file = os.path.join(GLASSDOOR_DIR, f'glassdoor_{year}_tickers.json')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved ticker mapping to {output_file}")


def main():
    """Main function to convert Glassdoor companies to tickers."""
    print("Glassdoor Company Name to Ticker Converter")
    print("=" * 60)
    
    # Get year input
    while True:
        try:
            year_input = input("Enter the year to process (2009-2025) or 'all' for all years: ").strip().lower()
            
            if year_input == 'all':
                years = list(range(2009, 2026))
                break
            else:
                year = int(year_input)
                if 2009 <= year <= 2025:
                    years = [year]
                    break
                else:
                    print(f"Error: Year must be between 2009 and 2025. Please try again.")
        except ValueError:
            print(f"Error: '{year_input}' is not a valid year. Please enter a number between 2009 and 2025, or 'all'.")
        except KeyboardInterrupt:
            print("\n\nConverter cancelled by user.")
            return
    
    # Process each year
    for year in years:
        print(f"\n{'='*60}")
        print(f"Processing year {year}")
        print(f"{'='*60}")
        
        results = convert_glassdoor_year_to_tickers(year)
        
        if results:
            # Print summary
            print(f"\n{'='*60}")
            print(f"Year {year} Summary:")
            print(f"  Total companies: {results['stats']['total']}")
            print(f"  Matched to tickers: {results['stats']['matched']}")
            print(f"  Unmatched/Private: {results['stats']['unmatched']}")
            print(f"  Match rate: {results['stats']['match_rate']:.1f}%")
            
            # Save results
            save_ticker_mapping(results, year)
            
            # Show sample matches
            if results['matched']:
                print(f"\nSample matches (first 10):")
                for match in results['matched'][:10]:
                    print(f"  {match['glassdoor_name']} -> {match['ticker']}")
        
        # Delay between years
        if len(years) > 1 and year != years[-1]:
            print("\nWaiting 2 seconds before next year...")
            time.sleep(2)
    
    if len(years) > 1:
        print(f"\n{'='*60}")
        print(f"Completed processing {len(years)} years!")
        print(f"{'='*60}")


if __name__ == '__main__':
    main()

