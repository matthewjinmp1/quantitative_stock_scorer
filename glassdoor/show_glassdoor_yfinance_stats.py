"""
Show statistics about Glassdoor company name to ticker matching for each year.
"""
import json
import os
from typing import Dict, List, Optional
from datetime import datetime

# Get project root directory (1 level up from this script, since script is in glassdoor/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
GLASSDOOR_DIR = SCRIPT_DIR  # Script is already in glassdoor directory
TICKERS_DIR = os.path.join(GLASSDOOR_DIR, 'data', 'tickers_yfinance')


def load_ticker_mapping(year: int) -> Optional[Dict]:
    """Load ticker mapping results from JSON file if it exists."""
    ticker_file = os.path.join(TICKERS_DIR, f'glassdoor_{year}_tickers.json')
    
    if not os.path.exists(ticker_file):
        return None
    
    try:
        with open(ticker_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load ticker mapping for {year}: {e}")
        return None


def get_all_years() -> List[int]:
    """Get all years that have ticker mapping files."""
    years = []
    for year in range(2009, 2026):
        ticker_file = os.path.join(TICKERS_DIR, f'glassdoor_{year}_tickers.json')
        if os.path.exists(ticker_file):
            years.append(year)
    return sorted(years)


def get_statistics_for_year(year: int) -> Optional[Dict]:
    """Get statistics for a specific year."""
    data = load_ticker_mapping(year)
    
    if not data:
        return None
    
    stats = data.get('stats', {})
    return {
        'year': year,
        'total': stats.get('total', 0),
        'matched': stats.get('matched', 0),
        'unmatched': stats.get('unmatched', 0),
        'match_rate': stats.get('match_rate', 0.0)
    }


def show_statistics_table(years: List[int], detailed: bool = False):
    """Display statistics in a formatted table."""
    print("\n" + "=" * 80)
    print("Glassdoor Company Name to Ticker Matching Statistics")
    print("=" * 80)
    print()
    
    # Header
    print(f"{'Year':<8} {'Total':<10} {'Matched':<12} {'Unmatched':<12} {'Match Rate':<12}")
    print("-" * 80)
    
    total_companies = 0
    total_matched = 0
    total_unmatched = 0
    
    stats_list = []
    
    for year in years:
        stats = get_statistics_for_year(year)
        if stats:
            stats_list.append(stats)
            total_companies += stats['total']
            total_matched += stats['matched']
            total_unmatched += stats['unmatched']
            
            match_rate_str = f"{stats['match_rate']:.1f}%"
            print(f"{stats['year']:<8} {stats['total']:<10} {stats['matched']:<12} "
                  f"{stats['unmatched']:<12} {match_rate_str:<12}")
    
    # Summary
    print("-" * 80)
    if total_companies > 0:
        overall_match_rate = (total_matched / total_companies) * 100
        print(f"{'TOTAL':<8} {total_companies:<10} {total_matched:<12} "
              f"{total_unmatched:<12} {overall_match_rate:.1f}%")
    
    print("=" * 80)
    
    # Additional statistics
    if stats_list:
        print(f"\nSummary:")
        print(f"  Years processed: {len(stats_list)}")
        print(f"  Total companies: {total_companies}")
        print(f"  Total matched: {total_matched} ({overall_match_rate:.1f}%)")
        print(f"  Total unmatched: {total_unmatched} ({100-overall_match_rate:.1f}%)")
        
        # Best and worst years
        best_year = max(stats_list, key=lambda x: x['match_rate'])
        worst_year = min(stats_list, key=lambda x: x['match_rate'])
        
        print(f"\n  Best year: {best_year['year']} ({best_year['match_rate']:.1f}% match rate)")
        print(f"  Worst year: {worst_year['year']} ({worst_year['match_rate']:.1f}% match rate)")
        
        # Average match rate
        avg_match_rate = sum(s['match_rate'] for s in stats_list) / len(stats_list)
        print(f"  Average match rate: {avg_match_rate:.1f}%")
    
    # Show detailed unmatched companies if requested
    if detailed:
        print("\n" + "=" * 80)
        print("Unmatched Companies by Year")
        print("=" * 80)
        
        for year in years:
            data = load_ticker_mapping(year)
            if data and data.get('unmatched'):
                print(f"\n{year} ({len(data['unmatched'])} unmatched):")
                # Show first 20 unmatched companies
                for i, company in enumerate(data['unmatched'][:20], 1):
                    print(f"  {i}. {company}")
                if len(data['unmatched']) > 20:
                    print(f"  ... and {len(data['unmatched']) - 20} more")


def show_year_details(year: int):
    """Show detailed information for a specific year."""
    data = load_ticker_mapping(year)
    
    if not data:
        print(f"\nNo data found for year {year}")
        return
    
    stats = data.get('stats', {})
    matched = data.get('matched', [])
    unmatched = data.get('unmatched', [])
    
    print(f"\n{'=' * 80}")
    print(f"Year {year} Details")
    print(f"{'=' * 80}")
    print(f"\nTotal companies: {stats.get('total', 0)}")
    print(f"Matched: {stats.get('matched', 0)} ({stats.get('match_rate', 0):.1f}%)")
    print(f"Unmatched: {stats.get('unmatched', 0)}")
    
    if matched:
        print(f"\nMatched Companies ({len(matched)}):")
        for i, match in enumerate(matched, 1):
            ticker = match.get('ticker', 'N/A')
            company_name = match.get('glassdoor_name', 'N/A')
            ipo_date = match.get('ipo_date', 'N/A')
            print(f"  {i:3}. {company_name:<40} -> {ticker:<8} (IPO: {ipo_date})")
    
    if unmatched:
        print(f"\nUnmatched Companies ({len(unmatched)}):")
        for i, company in enumerate(unmatched, 1):
            print(f"  {i:3}. {company}")


def main():
    """Main function to display statistics."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Show Glassdoor company name to ticker matching statistics'
    )
    parser.add_argument(
        '--year', '-y',
        type=int,
        help='Show detailed information for a specific year (2009-2025)'
    )
    parser.add_argument(
        '--detailed', '-d',
        action='store_true',
        help='Show detailed unmatched companies list'
    )
    parser.add_argument(
        '--all-years', '-a',
        action='store_true',
        help='Show statistics for all available years (default)'
    )
    
    args = parser.parse_args()
    
    # Get available years
    available_years = get_all_years()
    
    if not available_years:
        print("No ticker mapping files found in glassdoor/data/tickers_yfinance/")
        print("Run convert_glassdoor_yfinance.py first to generate mappings.")
        return
    
    # If specific year requested, show details
    if args.year:
        if args.year in available_years:
            show_year_details(args.year)
        else:
            print(f"Year {args.year} not found in available years: {available_years}")
    else:
        # Show statistics table
        show_statistics_table(available_years, detailed=args.detailed)


if __name__ == '__main__':
    main()

