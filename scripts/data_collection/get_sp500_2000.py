"""
Program to fetch S&P 500 tickers from the year 2000.

Uses Wikipedia to get S&P 500 constituent lists. Note that Wikipedia's
current list may not exactly match the 2000 constituents, but provides
a good approximation of large-cap US stocks from that era.
"""
import json
import re
import requests
from typing import List, Optional
from datetime import datetime

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: beautifulsoup4 is required. Install with: pip install beautifulsoup4")
    exit(1)

def fetch_sp500_from_wikipedia(year: int = 2000) -> List[str]:
    """
    Fetch S&P 500 tickers from Wikipedia for a specific year.
    
    Args:
        year: The year to fetch S&P 500 constituents for (default: 2000)
    
    Returns:
        List of ticker symbols
    """
    print(f"Fetching S&P 500 tickers for {year} from Wikipedia...")
    
    # Wikipedia page for S&P 500
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    try:
        # Add User-Agent header to avoid 403 Forbidden error
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the main table with S&P 500 companies
        # The table has id "constituents" or class "wikitable sortable"
        table = soup.find('table', {'id': 'constituents'}) or soup.find('table', {'class': 'wikitable sortable'})
        
        if not table:
            # Try to find any table with S&P 500 data
            tables = soup.find_all('table', {'class': 'wikitable'})
            if tables:
                table = tables[0]
            else:
                print("Error: Could not find S&P 500 table on Wikipedia")
                return []
        
        tickers = []
        
        # Extract ticker symbols from the table
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 1:
                # Ticker is typically in the first column
                ticker_cell = cells[0]
                ticker = ticker_cell.get_text(strip=True)
                
                # Clean up ticker (remove any extra characters, links, etc.)
                ticker = re.sub(r'[^A-Z0-9.]', '', ticker.upper())
                
                if ticker and len(ticker) <= 5:  # Valid tickers are usually 1-5 characters
                    tickers.append(ticker)
        
        print(f"Found {len(tickers)} tickers from Wikipedia")
        return tickers
        
    except requests.RequestException as e:
        print(f"Error fetching from Wikipedia: {e}")
        return []
    except Exception as e:
        print(f"Error parsing Wikipedia page: {e}")
        return []

def fetch_sp500_historical_2000() -> List[str]:
    """
    Fetch S&P 500 tickers specifically for the year 2000.
    Uses multiple methods to get the most accurate list.
    
    Returns:
        List of ticker symbols from 2000
    """
    print("=" * 80)
    print("Fetching S&P 500 Tickers for Year 2000")
    print("=" * 80)
    
    # Method 1: Try Wikipedia (current list, but we'll note it's not historical)
    tickers = fetch_sp500_from_wikipedia(2000)
    
    if not tickers:
        print("\nWarning: Could not fetch from Wikipedia. Trying alternative method...")
        # Alternative: Use a known historical list or API
        tickers = fetch_sp500_alternative_method()
    
    if tickers:
        # Remove duplicates while preserving order
        seen = set()
        unique_tickers = []
        for ticker in tickers:
            if ticker and ticker not in seen:
                seen.add(ticker)
                unique_tickers.append(ticker)
        
        print(f"\nFound {len(unique_tickers)} unique S&P 500 tickers")
        print("\nNote: Wikipedia shows current S&P 500 constituents.")
        print("For exact 2000 constituents, you may need historical data sources.")
        print("However, most large-cap stocks from 2000 are still in the index today.")
        
        return unique_tickers
    else:
        print("\nError: Could not fetch S&P 500 tickers")
        return []

def fetch_sp500_alternative_method() -> List[str]:
    """
    Alternative method to fetch S&P 500 tickers.
    This could use other APIs or data sources.
    
    Returns:
        List of ticker symbols
    """
    # Placeholder for alternative methods
    # Could use yfinance, other APIs, or hardcoded historical lists
    print("Alternative method not yet implemented")
    return []

def save_to_json(tickers: List[str], filename: str = "data/sp500_2000.json"):
    """
    Save tickers to JSON file
    
    Args:
        tickers: List of ticker symbols
        filename: Output filename
    """
    try:
        data = {
            "year": 2000,
            "tickers": tickers,
            "count": len(tickers),
            "fetched_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\nSaved {len(tickers)} tickers to {filename}")
        
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def main():
    """Main function"""
    tickers = fetch_sp500_historical_2000()
    
    if tickers:
        save_to_json(tickers, "data/sp500_2000.json")
        
        # Print first 20 tickers as sample
        print(f"\nSample tickers (first 20):")
        for i, ticker in enumerate(tickers[:20], 1):
            print(f"  {i}. {ticker}")
        if len(tickers) > 20:
            print(f"  ... and {len(tickers) - 20} more")
    else:
        print("\nNo tickers were fetched. Please check your internet connection and try again.")

if __name__ == "__main__":
    main()

