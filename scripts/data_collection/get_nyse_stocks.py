"""
Program to fetch all NYSE stocks from QuickFS API and save to nyse.json
"""
import json
from quickfs import QuickFS
from config import QUICKFS_API_KEY

# QuickFS API Configuration
API_KEY = QUICKFS_API_KEY

def get_nyse_stocks():
    """
    Fetch all NYSE stocks from QuickFS API
    
    Returns:
        List of ticker symbols for NYSE stocks
    """
    try:
        print("Fetching NYSE stocks from QuickFS API...")
        client = QuickFS(API_KEY)
        
        # Get all companies listed on NYSE
        nyse_companies = client.get_supported_companies(country='US', exchange='NYSE')
        
        if not nyse_companies:
            print("Warning: No NYSE companies returned from API")
            return []
        
        # Extract ticker symbols (remove :US suffix if present)
        tickers = []
        for company in nyse_companies:
            if isinstance(company, str):
                # If it's already a string (ticker symbol)
                ticker = company.replace(':US', '').strip()
                if ticker:
                    tickers.append(ticker)
            elif isinstance(company, dict):
                # If it's a dictionary, try to extract ticker from common fields
                ticker = company.get('ticker') or company.get('symbol') or company.get('code')
                if ticker:
                    ticker = str(ticker).replace(':US', '').strip()
                    if ticker:
                        tickers.append(ticker)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tickers = []
        for ticker in tickers:
            if ticker not in seen:
                seen.add(ticker)
                unique_tickers.append(ticker)
        
        print(f"Found {len(unique_tickers)} unique NYSE ticker(s)")
        return unique_tickers
        
    except Exception as e:
        print(f"Error fetching NYSE stocks: {e}")
        return []

def save_to_json(tickers, filename='data/nyse.json'):
    """
    Save ticker symbols to JSON file
    
    Args:
        tickers: List of ticker symbols
        filename: Output filename
    """
    try:
        data = {
            "tickers": sorted(tickers)  # Sort alphabetically for consistency
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\nNYSE stocks saved to {filename}")
        print(f"Total tickers: {len(tickers)}")
    except Exception as e:
        print(f"Error saving to {filename}: {e}")

def main():
    """
    Main function to fetch NYSE stocks and save to JSON
    """
    print("=" * 60)
    print("Fetching NYSE Stocks from QuickFS API")
    print("=" * 60)
    
    # Fetch NYSE stocks
    tickers = get_nyse_stocks()
    
    if not tickers:
        print("No tickers found. Please check your API key and connection.")
        return
    
    # Save to JSON file
    save_to_json(tickers, 'data/nyse.json')
    
    print("\nDone!")

if __name__ == "__main__":
    main()

