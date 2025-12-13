"""
Convert Glassdoor company names to stock tickers using yfinance.
Checks if companies were public at the time of the list.
Handles delisted companies by checking historical data.
"""
import json
import os
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import pandas as pd
import yfinance as yf

# Get project root directory (2 levels up from this script)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
GLASSDOOR_DIR = os.path.join(PROJECT_ROOT, 'glassdoor')
COMPANIES_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'companies')
TICKERS_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'tickers_yfinance')


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
        # Missing companies from 2023 list
        'Marvell Technology': 'MRVL',
        'Marvell': 'MRVL',
        'FedEx Services': 'FDX',
        'FedEx': 'FDX',
        'Fedex': 'FDX',
        'Amd': 'AMD',
        'AMD': 'AMD',
        'Red Hat': 'RHT',  # Acquired by IBM but was public in 2023
        'Asana': 'ASAN',
        'Con Edison': 'ED',
        'Consolidated Edison': 'ED',
        'Regeneron': 'REGN',
        'Regeneron Pharmaceuticals': 'REGN',
        'Merck': 'MRK',
        'Merck & Co': 'MRK',
        'Booz Allen Hamilton': 'BAH',
        'Atlassian': 'TEAM',
        'Hewlett Packard Enterprise': 'HPE',
        'Hewlett Packard Enterprise Hpe': 'HPE',
        'HPE': 'HPE',
        'Spotify': 'SPOT',
        'Athenahealth': 'ATHN',  # Acquired but was public
        'Athena Health': 'ATHN',
        'Rbc': 'RY',  # Royal Bank of Canada
        'Royal Bank Of Canada': 'RY',
        'Schneider Electric': 'SBGSY',  # ADR
        'HP Inc': 'HPQ',
        'HP': 'HPQ',
        'Live Nation Entertainment': 'LYV',
        'Live Nation': 'LYV',
        'Toast Inc': 'TOST',
        'Toast': 'TOST',
        'Crowdstrike': 'CRWD',
        'CrowdStrike': 'CRWD',
        'Qualtrics': 'XM',  # Acquired by SAP but was public
        'Box': 'BOX',
        'Servicenow': 'NOW',
        'ServiceNow': 'NOW',
        'Mongodb': 'MDB',
        'MongoDB': 'MDB',
        'Fortinet': 'FTNT',
        'Garmin': 'GRMN',
        'Mastercard': 'MA',
        'Procore Technologies': 'PCOR',
        'Procore': 'PCOR',
        'Cummins': 'CMI',
        'Dell Technologies': 'DELL',
        'Dell': 'DELL',
        'Illumina': 'ILMN',
        'Workday': 'WDAY',
        'Blackrock': 'BLK',
        'BlackRock': 'BLK',
        'Agilent Technologies': 'A',
        'Agilent': 'A',
        'Intel Corporation': 'INTC',
        'Intel': 'INTC',
        'Epam Systems': 'EPAM',
        'EPAM': 'EPAM',
        'United Rentals': 'URI',
        'Twilio': 'TWLO',
        'Hilti North America': None,  # Private subsidiary
        'Hilti': None,  # Private
        'Cielo': None,  # Brazilian company, check if ADR exists
        '2020 Companies': None,  # Private
        'Black & Veatch': None,  # Private
        'Burns & Mcdonnell': None,  # Private
        'Cengage Group': None,  # Private
        'Armanino': None,  # Private accounting firm
        'Blue Cross & Blue Shield Of North Carolina': None,  # Non-profit
        'Mathworks': None,  # Private
        'MathWorks': None,  # Private
        'Fidelity Investments': None,  # Private
        'Gainsight': None,  # Private
        'Medical Solutions': None,  # Private
        'Berkshire Hathaway Homeservices': None,  # Private subsidiary
        'Exp Realty': None,  # Private
        'The Lego Group': None,  # Private
        'Lego': None,  # Private
        'Onedigital': None,  # Private
        'OneDigital': None,  # Private
        'Turner Construction': None,  # Private
        'Rsm': None,  # Private (RSM US)
        'RSM': None,  # Private
        'Zeigler Auto Group': None,  # Private
        'Curriculum Associates': None,  # Private
        'Houston Methodist': None,  # Non-profit
        'Coldwell Banker': None,  # Private (Realogy)
        'Mathnasium': None,  # Private
        'Crew Carwash': None,  # Private
        'Veterans United Home Loans': None,  # Private
        'Cardinal Group Companies': None,  # Private
        'Avanade': None,  # Private (Accenture-Microsoft JV)
        'Md Anderson Cancer Center': None,  # Non-profit
        'Tampa General Hospital': None,  # Non-profit
        'Johns Hopkins University Applied Physics Laboratory': None,  # Non-profit
        # Additional common companies
        'Genentech': 'DNA',  # Acquired by Roche but was public
        'Whole Foods Market': 'WFM',  # Acquired by Amazon but was public
        'Continental Airlines': 'CAL',  # Merged with United
        'Citrix': 'CTXS',  # Acquired by Cloud Software Group but was public
        'National Instruments': 'NATI',  # Acquired by Emerson but was public
        'Novell': None,  # Acquired
        'Blizzard Entertainment': 'ATVI',  # Now part of Microsoft but was public
        'Ellie Mae': 'ELLI',  # Acquired by Intercontinental Exchange but was public
        'Ultimate Software': 'ULTI',  # Now UKG after merger but was public
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
    
    # Try partial matching (if company name contains key or vice versa)
    company_lower = company_name.lower()
    for key, value in known_mappings.items():
        key_lower = key.lower()
        # Check if one contains the other (for variations like "HP Inc" vs "HP")
        if key_lower in company_lower or company_lower in key_lower:
            # Make sure it's a reasonable match (not just a single letter)
            if len(key_lower) >= 3 and len(company_lower) >= 3:
                return value
    
    # Try matching after removing common words and normalizing
    normalized_company = normalize_company_name_for_search(company_name).lower()
    for key, value in known_mappings.items():
        normalized_key = normalize_company_name_for_search(key).lower()
        if normalized_key == normalized_company:
            return value
        # Also try partial match on normalized
        if normalized_key in normalized_company or normalized_company in normalized_key:
            if len(normalized_key) >= 3 and len(normalized_company) >= 3:
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
        'META': ('FB', 2022),  # Facebook was FB until 2022, then changed to META
    }
    
    # Check if we need to use historical ticker
    if ticker in historical_tickers:
        old_ticker, change_year = historical_tickers[ticker]
        if year < change_year:
            ticker = old_ticker
    
    try:
        stock = yf.Ticker(ticker)
        
        # PRIORITY 1: Try to get historical data first (most reliable for delisted companies)
        # This works even if the company is no longer public today
        try:
            # First, try to get max history to see if company ever existed
            full_hist = stock.history(period="max")
            
            if full_hist is not None and len(full_hist) > 0:
                # Company has historical data - check if it was trading in the target year
                first_date = full_hist.index[0]
                last_date = full_hist.index[-1]
                
                # Get years from dates
                if isinstance(full_hist.index, pd.DatetimeIndex):
                    first_year = first_date.year
                    last_year = last_date.year
                    
                    # Check if company was trading during the target year
                    if first_year <= year <= last_year:
                        # Company was trading during this period - check specific year
                        year_data = full_hist[full_hist.index.year == year]
                        if len(year_data) > 0:
                            # Company was definitely trading in this year
                            first_date_str = first_date.strftime('%Y-%m-%d')
                            return True, f"First trade: {first_date_str} (historical data)"
                        elif first_year <= year:
                            # Company started before or during the year, assume it was public
                            first_date_str = first_date.strftime('%Y-%m-%d')
                            return True, f"First trade: {first_date_str} (historical data, may be delisted)"
                else:
                    # Fallback: try to parse dates
                    try:
                        first_year = pd.to_datetime(first_date).year
                        last_year = pd.to_datetime(last_date).year
                        if first_year <= year <= last_year:
                            first_date_str = str(first_date)
                            return True, f"First trade: {first_date_str} (historical data)"
                    except:
                        pass
        except Exception as e:
            # Historical data check failed, continue to other methods
            pass
        
        # PRIORITY 2: Try to get info (may fail for delisted companies, but worth trying)
        try:
            info = stock.info
        except:
            info = None
        
        # If we have info, use it
        if info:
            # Get IPO date from info
            ipo_date = info.get('ipoDate')
            if ipo_date:
                # Parse IPO date
                if isinstance(ipo_date, (int, float)):
                    try:
                        ipo_datetime = datetime.fromtimestamp(ipo_date / 1000)  # Assume milliseconds
                        ipo_year = ipo_datetime.year
                    except:
                        # Try to get from historical data instead
                        try:
                            hist = stock.history(period="max")
                            if hist is not None and len(hist) > 0:
                                first_date = hist.index[0]
                                first_year = first_date.year if hasattr(first_date, 'year') else pd.to_datetime(first_date).year
                                if first_year <= year:
                                    return True, f"First trade: {str(first_date)}"
                        except:
                            pass
                        return False, f"Could not parse IPO timestamp: {ipo_date}"
                elif isinstance(ipo_date, str):
                    try:
                        for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z']:
                            try:
                                ipo_datetime = datetime.strptime(ipo_date, fmt)
                                ipo_year = ipo_datetime.year
                                break
                            except:
                                continue
                        else:
                            # If parsing failed, try historical data
                            try:
                                hist = stock.history(period="max")
                                if hist is not None and len(hist) > 0:
                                    first_date = hist.index[0]
                                    first_year = first_date.year if hasattr(first_date, 'year') else pd.to_datetime(first_date).year
                                    if first_year <= year:
                                        return True, f"First trade: {str(first_date)}"
                            except:
                                pass
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
        
        # PRIORITY 3: If no info but we might have historical data, try one more time
        if not info:
            try:
                hist = stock.history(period="max")
                if hist is not None and len(hist) > 0:
                    first_date = hist.index[0]
                    if isinstance(hist.index, pd.DatetimeIndex):
                        first_year = first_date.year
                        if first_year <= year:
                            # Check if company was trading in the target year
                            year_data = hist[hist.index.year == year]
                            if len(year_data) > 0 or first_year <= year:
                                first_date_str = first_date.strftime('%Y-%m-%d')
                                return True, f"First trade: {first_date_str} (delisted, no current info)"
                    else:
                        # Fallback
                        try:
                            first_year = pd.to_datetime(first_date).year
                            if first_year <= year:
                                return True, f"First trade: {str(first_date)} (delisted, no current info)"
                        except:
                            pass
            except:
                pass
            return False, "No info or historical data available"
        
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
    
    # If still no match, try removing common words and searching again
    if ticker is None:
        # Remove common prefixes/suffixes and try again
        words_to_remove = ['The', 'A', 'An', 'Inc', 'Corp', 'LLC', 'Ltd', 'Limited', 
                          'Company', 'Co', 'Technologies', 'Technology', 'Tech', 
                          'Services', 'Service', 'Group', 'Holdings', 'Systems', 'System']
        cleaned_name = company_name
        for word in words_to_remove:
            # Remove word if it's at the end
            if cleaned_name.endswith(' ' + word) or cleaned_name.endswith(' ' + word + '.'):
                cleaned_name = cleaned_name[:-len(word)-1].strip()
            # Remove word if it's at the beginning
            if cleaned_name.startswith(word + ' '):
                cleaned_name = cleaned_name[len(word)+1:].strip()
        if cleaned_name != company_name:
            ticker = try_ticker_from_name(cleaned_name, data_mapping)
    
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


def process_single_company(company_name: str, year: int, data_mapping: Dict[str, str], 
                          index: int, total: int) -> Tuple[Optional[Dict], Optional[str], int]:
    """
    Process a single company to find its ticker.
    
    Returns:
        (result_dict_or_None, company_name, index)
    """
    try:
        result = find_ticker_for_company(company_name, year, data_mapping)
        
        if result:
            return result, company_name, index
        else:
            return None, company_name, index
    except Exception as e:
        print(f"  ✗ Error processing {company_name}: {e}")
        return None, company_name, index


def convert_glassdoor_year_to_tickers(year: int, max_workers: int = 10, use_cache: bool = True) -> Dict:
    """
    Convert Glassdoor company names to tickers for a specific year using multithreading.
    Uses caching to only re-process previously unmatched companies.
    
    Args:
        year: Year to process
        max_workers: Maximum number of worker threads (default: 10)
        use_cache: Whether to use cached results and only process unmatched companies (default: True)
    
    Returns:
        Dict with 'year', 'companies', 'matched', 'unmatched', 'stats'
    """
    # Load existing results if available
    cached_results = None
    if use_cache:
        cached_results = load_existing_ticker_mapping(year)
        if cached_results:
            print(f"Found cached results for year {year}")
            print(f"  Previously matched: {len(cached_results.get('matched', []))}")
            print(f"  Previously unmatched: {len(cached_results.get('unmatched', []))}")
    
    # Build company name mapping from existing data files
    print("Building company name mapping from existing stock data...")
    data_mapping = build_company_name_mapping_from_data()
    print(f"Loaded {len(data_mapping)} company name mappings from data files")
    
    # Load Glassdoor companies for the year
    glassdoor_file = os.path.join(COMPANIES_DIR, f'glassdoor_{year}_companies.json')
    
    if not os.path.exists(glassdoor_file):
        print(f"Error: {glassdoor_file} not found")
        return None
    
    with open(glassdoor_file, 'r', encoding='utf-8') as f:
        glassdoor_companies = json.load(f)
    
    # Determine which companies to process
    companies_to_process = []
    cached_matched = {}
    cached_unmatched = set()
    
    if cached_results:
        # Build lookup dictionaries from cache
        for match in cached_results.get('matched', []):
            cached_matched[match.get('glassdoor_name', '')] = match
        
        for unmatched_name in cached_results.get('unmatched', []):
            cached_unmatched.add(unmatched_name)
        
        # Only process companies that were previously unmatched
        companies_to_process = [c for c in glassdoor_companies if c in cached_unmatched]
        
        print(f"\nUsing cache: {len(cached_matched)} already matched, {len(cached_unmatched)} to re-process")
        print(f"Processing {len(companies_to_process)} previously unmatched companies using {max_workers} threads...")
    else:
        # No cache, process all companies
        companies_to_process = glassdoor_companies
        print(f"\nProcessing {len(glassdoor_companies)} companies for year {year} using {max_workers} threads...")
    
    # If nothing to process, return cached results
    if not companies_to_process and cached_results:
        print("All companies already processed. Using cached results.")
        return cached_results
    
    matched = []
    unmatched = []
    results_lock = threading.Lock()
    completed_count = 0
    total_count = len(companies_to_process)
    
    # Use ThreadPoolExecutor to process companies in parallel
    if companies_to_process:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_company = {
                executor.submit(process_single_company, company_name, year, data_mapping, i, total_count): company_name
                for i, company_name in enumerate(companies_to_process, 1)
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_company):
                company_name = future_to_company[future]
                try:
                    result, processed_name, index = future.result()
                    
                    with results_lock:
                        completed_count += 1
                        if result:
                            matched.append(result)
                            print(f"[{completed_count}/{total_count}] ✓ {processed_name} -> {result['ticker']} (IPO: {result['ipo_date']})")
                        else:
                            unmatched.append(processed_name)
                            print(f"[{completed_count}/{total_count}] ✗ {processed_name} (No match or not public)")
                except Exception as e:
                    with results_lock:
                        completed_count += 1
                        unmatched.append(company_name)
                        print(f"[{completed_count}/{total_count}] ✗ {company_name} (Error: {e})")
    
    # Merge cached and new results
    if cached_results:
        # Add cached matched companies
        for match in cached_results.get('matched', []):
            if match.get('glassdoor_name') not in [m.get('glassdoor_name') for m in matched]:
                matched.append(match)
        
        # Remove companies from unmatched if they're now matched
        new_unmatched = [u for u in unmatched if u not in [m.get('glassdoor_name') for m in matched]]
        
        # Add companies that are still unmatched (from cache) but weren't re-processed
        for cached_unmatched_name in cached_unmatched:
            if cached_unmatched_name not in companies_to_process:
                new_unmatched.append(cached_unmatched_name)
        
        unmatched = new_unmatched
    
    # Sort matched and unmatched to maintain original order
    matched_dict = {m['glassdoor_name']: m for m in matched}
    matched_sorted = [matched_dict[name] for name in glassdoor_companies if name in matched_dict]
    unmatched_sorted = [name for name in glassdoor_companies if name not in matched_dict]
    
    return {
        'year': year,
        'companies': glassdoor_companies,
        'matched': matched_sorted,
        'unmatched': unmatched_sorted,
        'stats': {
            'total': len(glassdoor_companies),
            'matched': len(matched_sorted),
            'unmatched': len(unmatched_sorted),
            'match_rate': len(matched_sorted) / len(glassdoor_companies) * 100 if glassdoor_companies else 0
        }
    }


def load_existing_ticker_mapping(year: int) -> Optional[Dict]:
    """Load existing ticker mapping results from JSON file if it exists."""
    output_file = os.path.join(TICKERS_DIR, f'glassdoor_{year}_tickers.json')
    
    if not os.path.exists(output_file):
        return None
    
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load existing mapping for {year}: {e}")
        return None


def save_ticker_mapping(results: Dict, year: int):
    """Save ticker mapping results to JSON file."""
    # Ensure tickers directory exists
    os.makedirs(TICKERS_DIR, exist_ok=True)
    output_file = os.path.join(TICKERS_DIR, f'glassdoor_{year}_tickers.json')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved ticker mapping to {output_file}")


def main():
    """Main function to convert Glassdoor companies to tickers."""
    print("Glassdoor Company Name to Ticker Converter (Multithreaded)")
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
    
    # Get number of threads
    while True:
        try:
            threads_input = input("Enter number of threads (default: 10, recommended: 5-20): ").strip()
            if not threads_input:
                max_workers = 10
                break
            max_workers = int(threads_input)
            if 1 <= max_workers <= 50:
                break
            else:
                print("Error: Number of threads must be between 1 and 50. Please try again.")
        except ValueError:
            print("Error: Please enter a valid number.")
        except KeyboardInterrupt:
            print("\n\nConverter cancelled by user.")
            return
    
    # Ask about cache usage
    use_cache = True
    try:
        cache_input = input("Use cached results and only re-process unmatched companies? (Y/n): ").strip().lower()
        if cache_input in ['n', 'no']:
            use_cache = False
            print("Cache disabled. Will process all companies.")
        else:
            print("Cache enabled. Will only re-process previously unmatched companies.")
    except KeyboardInterrupt:
        print("\n\nConverter cancelled by user.")
        return
    
    # Process each year
    for year in years:
        print(f"\n{'='*60}")
        print(f"Processing year {year}")
        print(f"{'='*60}")
        
        start_time = time.time()
        results = convert_glassdoor_year_to_tickers(year, max_workers=max_workers, use_cache=use_cache)
        elapsed_time = time.time() - start_time
        
        if results:
            # Print summary
            print(f"\n{'='*60}")
            print(f"Year {year} Summary:")
            print(f"  Total companies: {results['stats']['total']}")
            print(f"  Matched to tickers: {results['stats']['matched']}")
            print(f"  Unmatched/Private: {results['stats']['unmatched']}")
            print(f"  Match rate: {results['stats']['match_rate']:.1f}%")
            print(f"  Processing time: {elapsed_time:.2f} seconds")
            
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

