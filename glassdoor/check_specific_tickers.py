"""
Check specific tickers and company names in the data files.
"""
import json
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

def find_ticker_in_data(ticker: str):
    """Find a ticker in the data files."""
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

def search_company_name_in_data(search_term: str):
    """Search for a company name in the data files."""
    results = []
    search_lower = search_term.lower()
    
    for data_file in ['nyse_data.jsonl', 'nasdaq_data.jsonl']:
        file_path = os.path.join(DATA_DIR, data_file)
        if not os.path.exists(file_path):
            continue
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        stock = json.loads(line)
                        company_name = stock.get('company_name', '')
                        ticker = stock.get('symbol', '')
                        if search_lower in company_name.lower() or company_name.lower() in search_lower:
                            results.append({
                                'ticker': ticker,
                                'company_name': company_name
                            })
                    except:
                        continue
    
    return results

# Check specific cases
print("="*80)
print("Checking Specific Tickers and Companies")
print("="*80)

# 1. Check Northwestern Mutual vs NWE
print("\n1. Northwestern Mutual / NWE:")
nwe_info = find_ticker_in_data('NWE')
if nwe_info.get('found'):
    print(f"   NWE found: {nwe_info['company_name']}")
    print(f"   [WARNING] This is NorthWestern Corporation (utility), NOT Northwestern Mutual (insurance)")
    print(f"   [FALSE POSITIVE] Northwestern Mutual is a mutual company, not publicly traded")

# 2. Check Genentech / DNA
print("\n2. Genentech / DNA:")
dna_info = find_ticker_in_data('DNA')
if dna_info.get('found'):
    print(f"   DNA found: {dna_info['company_name']}")
    # Check if it has 2009 data
    print(f"   [OK] Ticker exists in data")
else:
    print(f"   DNA not found in data files")
    # Search for Genentech
    genentech_results = search_company_name_in_data('Genentech')
    if genentech_results:
        print(f"   Found companies with 'Genentech' in name:")
        for r in genentech_results:
            print(f"     - {r['ticker']}: {r['company_name']}")

# 3. Check CareerBuilder
print("\n3. CareerBuilder:")
careerbuilder_results = search_company_name_in_data('CareerBuilder')
if careerbuilder_results:
    print(f"   Found in data:")
    for r in careerbuilder_results:
        print(f"     - {r['ticker']}: {r['company_name']}")
else:
    print(f"   Not found in data files")
    print(f"   [OK] Likely private company - correctly unmatched")

# 4. Check Novell
print("\n4. Novell:")
novell_results = search_company_name_in_data('Novell')
if novell_results:
    print(f"   Found in data:")
    for r in novell_results:
        print(f"     - {r['ticker']}: {r['company_name']}")
else:
    print(f"   Not found in data files")
    print(f"   Note: Novell was acquired by Micro Focus in 2014, may have been delisted")

# 5. Check other potentially problematic matches
print("\n5. Checking other QuickFS matches that yfinance didn't find:")
quickfs_only = ['BAH', 'CTXS', 'EMC', 'JNPR', 'NATI', 'JWN', 'WFM']
for ticker in quickfs_only:
    info = find_ticker_in_data(ticker)
    if info.get('found'):
        print(f"   {ticker}: {info['company_name']} [OK] Valid match")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print("FALSE POSITIVE:")
print("  - Northwestern Mutual -> NWE (should be unmatched)")
print("\nPOTENTIALLY MISSED:")
print("  - Genentech (DNA) - check if exists and has 2009 data")
print("  - CareerBuilder - likely private, correctly unmatched")
print("  - Novell - likely delisted/acquired, correctly unmatched")

