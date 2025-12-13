"""
Scrape company names from Glassdoor's Best Places to Work list for any year (2009-2025)
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
from typing import List, Optional
from datetime import datetime

# Get project root directory (2 levels up from this script)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
GLASSDOOR_DIR = os.path.join(DATA_DIR, 'glassdoor')
COMPANIES_DIR = os.path.join(GLASSDOOR_DIR, 'companies')


def normalize_company_name(url_part: str) -> str:
    """Normalize company name from URL format to proper format."""
    # Convert URL format to readable name (e.g., "General-Mills" -> "General Mills")
    company_name = url_part.replace('-', ' ')
    # Title case each word
    words = company_name.split()
    company_name = ' '.join(word.capitalize() for word in words)
    # Fix special cases like "And" -> "&"
    company_name = company_name.replace(' And ', ' & ')
    # Fix known company name formats
    name_fixes = {
        'At & T': 'AT&T',
        'Usaa': 'USAA',
        'Emc': 'EMC',
        'Pwc': 'PwC',
        'Ti': 'TI',
        'Sap': 'SAP',
        'Netapp': 'NetApp',
        'Careerbuilder': 'CareerBuilder',
        'Mckinsey': 'McKinsey',
        'Factset': 'FactSet',
        'Mitre': 'MITRE',
        'Nike': 'NIKE',
        'Metlife': 'MetLife',
        'Us Army': 'US Army',
        'Fedex': 'FedEx',
        'Ey': 'EY',
        'Nestl': 'Nestlé',  # Fix encoding issue
        'Nestle': 'Nestlé',
        'Hubspot': 'HubSpot',
        'Docusign': 'DocuSign',
        'Vipkid': 'VIPKid',
        'Ukg': 'UKG',
        'Kronos': 'Kronos',
    }
    for old, new in name_fixes.items():
        if old in company_name:
            company_name = company_name.replace(old, new)
    return company_name


def parse_glassdoor_page(html_content: str, year: int, source_url: str = "") -> List[str]:
    """
    Parse company names from Glassdoor HTML content by extracting from link structure.
    
    Args:
        html_content: HTML content to parse
        year: Year of the list (for filtering)
        source_url: URL where content came from (for logging)
    
    Returns:
        List of company names
    """
    # Determine expected number of companies based on year
    # Years 2018-2025 have 100 companies, earlier years have 50
    if 2018 <= year <= 2025:
        expected_count = 100
        min_threshold = 80  # Minimum to consider a good list
    else:
        expected_count = 50
        min_threshold = 40  # Minimum to consider a good list
    soup = BeautifulSoup(html_content, 'html.parser')
    company_names = []
    
    # Method 1: Extract company names from Review links (most reliable)
    # Company names are in URLs like /Reviews/CompanyName-Reviews
    review_link_companies = []
    seen_hrefs = set()  # Track seen hrefs to avoid duplicates
    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        # Look for patterns like /Reviews/CompanyName-Reviews
        # Also handle variations like /Reviews/CompanyName or /Reviews/Company-Name-Reviews-E123
        match = re.search(r'/Reviews/([^/?-]+(?:-[^/?-]+)*?)(?:-Reviews(?:-E\d+)?|/|$)', href)
        if match:
            company_name = match.group(1)
            # Skip if it's a generic term
            if company_name.lower() in ['company', 'companies', 'reviews', 'employer', 'employers', 'employee']:
                continue
            
            # Normalize the href to avoid duplicates (remove trailing numbers/IDs)
            normalized_href = re.sub(r'-E\d+', '', href)
            normalized_href = re.sub(r'-Reviews.*', '-Reviews', normalized_href)
            if normalized_href in seen_hrefs:
                continue
            seen_hrefs.add(normalized_href)
            
            # Normalize company name
            company_name = normalize_company_name(company_name)
            
            if company_name and len(company_name) >= 3:
                review_link_companies.append(company_name)
    
    # Method 2: Try to find the ranking section and extract companies in order
    # Look for a section that contains "Best Places to Work" and the year
    ranking_section = None
    best_section = None
    best_count = 0
    
    year_str = str(year)
    for div in soup.find_all(['div', 'section', 'article']):
        text = div.get_text()
        # Check if this section contains ranking-related content
        if 'best places to work' in text.lower() and year_str in text:
            # Count how many review links are in this section
            review_links_in_section = div.find_all('a', href=re.compile(r'/Reviews/.*-Reviews'))
            if len(review_links_in_section) > best_count:
                best_count = len(review_links_in_section)
                best_section = div
    
    ranking_section = best_section
    
    # If we found a ranking section, extract companies from it in order
    if ranking_section:
        section_companies = []
        section_seen = set()
        for link in ranking_section.find_all('a', href=True):
            href = link.get('href', '')
            match = re.search(r'/Reviews/([^/?-]+(?:-[^/?-]+)*?)(?:-Reviews(?:-E\d+)?|/|$)', href)
            if match:
                company_name = match.group(1)
                if company_name.lower() in ['company', 'companies', 'reviews', 'employer', 'employers', 'employee']:
                    continue
                
                normalized_href = re.sub(r'-E\d+', '', href)
                normalized_href = re.sub(r'-Reviews.*', '-Reviews', normalized_href)
                if normalized_href in section_seen:
                    continue
                section_seen.add(normalized_href)
                
                company_name = normalize_company_name(company_name)
                
                if company_name and company_name not in section_companies:
                    section_companies.append(company_name)
        
        # If we got a good list from the section, use it
        if len(section_companies) >= min_threshold:
            company_names = section_companies
        else:
            company_names = review_link_companies
    else:
        company_names = review_link_companies
    
    # Remove duplicates while preserving order
    seen = set()
    unique_companies = []
    for company in company_names:
        company_lower = company.lower().strip()
        if company_lower and company_lower not in seen:
            seen.add(company_lower)
            unique_companies.append(company)
    
    # Filter to only include companies that look legitimate
    # Be less aggressive if we're close to the expected count
    filtered_companies = []
    exclude_terms = {'company reviews', 'reviews', 'company', 'companies', 'employer', 
                     'employers'}  # Generic navigation terms
    
    # Also exclude companies that appear in navigation/footer (not in the ranking)
    navigation_companies = {'target', 'walmart', 'macy', 'home depot', 'ibm', 
                           'microsoft', 'amazon', 'best buy reviews'}  # These appear in nav, not ranking
    
    # If we're close to expected count, be more lenient with filtering
    close_to_target = len(unique_companies) >= (expected_count - 2)
    
    for company in unique_companies:
        company_lower = company.lower()
        # Skip if it's a generic term (always filter these)
        if company_lower in exclude_terms:
            continue
        # Skip if it's a navigation company (unless we're close to target)
        if company_lower in navigation_companies:
            # Only skip if we have significantly more than expected companies
            # If we're close to target, keep it (might be in the ranking)
            if len(unique_companies) > expected_count + 5:  # More lenient threshold
                continue
        # Skip if it's too short or too long (but be lenient if close to target)
        if not close_to_target:  # Only apply strict length filter if we have plenty
            if not (3 <= len(company) <= 80):
                continue
        else:  # If close to target, only filter obviously wrong lengths
            if len(company) < 2 or len(company) > 100:
                continue
        # Must contain letters (always check this)
        if not any(c.isalpha() for c in company):
            continue
        filtered_companies.append(company)
    
    # If we have more than expected, try to identify the correct list by looking for ranking context
    if len(filtered_companies) > expected_count:
        # Try to find companies that appear in the ranking section specifically
        ranking_companies = []
        for div in soup.find_all(['div', 'section', 'article']):
            text = div.get_text()
            if 'best places to work' in text.lower() and year_str in text:
                # Extract companies from this section
                section_links = div.find_all('a', href=re.compile(r'/Reviews/.*-Reviews'))
                if len(section_links) >= min_threshold:  # Should have many company links
                    for link in section_links:
                        href = link.get('href', '')
                        match = re.search(r'/Reviews/([^/?-]+(?:-[^/?-]+)*?)(?:-Reviews|/|$)', href)
                        if match:
                            company_name = match.group(1)
                            if company_name.lower() not in exclude_terms:
                                company_name = normalize_company_name(company_name)
                                
                                if company_name and company_name not in ranking_companies:
                                    ranking_companies.append(company_name)
                    # If we found a good list from the ranking section, return it (up to expected count)
                    if len(ranking_companies) >= min_threshold:
                        return ranking_companies[:expected_count]
    
    # Return all filtered companies (up to expected count if we have more)
    if len(filtered_companies) > expected_count:
        return filtered_companies[:expected_count]
    return filtered_companies


def scrape_from_wayback_machine(year: int) -> List[str]:
    """
    Try to scrape from Wayback Machine archive.
    
    Args:
        year: Year of the list to scrape
    
    Returns:
        List of company names, or empty list if not found
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://web.archive.org/',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # Build URL for the year
    base_url = f'https://www.glassdoor.com/Award/Best-Places-to-Work-{year}-LST_KQ0,24.htm'
    
    # Generate snapshot dates - try dates around when the list would have been published
    # Lists are typically published in December of the previous year or early in the year
    snapshot_dates = []
    
    # Generate snapshot dates based on year
    current_year = datetime.now().year
    
    if year <= 2015:
        # For older years, try dates from the year after (when list would be archived)
        next_year = year + 1
        snapshot_dates = [
            f'{next_year}0115000000',  # January 15
            f'{next_year}0201000000',  # February 1
            f'{next_year}0301000000',  # March 1
            f'{year}1201000000',  # December of the year
            f'{next_year}0601000000',  # June
        ]
    elif year <= 2022:
        # For 2016-2022, try dates from the year after and the year itself
        next_year = year + 1
        snapshot_dates = [
            f'{next_year}0115000000',  # January 15 of next year
            f'{next_year}0201000000',  # February 1 of next year
            f'{next_year}0301000000',  # March 1 of next year
            f'{year}1201000000',  # December of the year
            f'{year}0101000000',  # January 1 of the year
        ]
    else:
        # For very recent years (2023+), try current year dates
        if year <= current_year:
            snapshot_dates = [
                f'{year}0115000000',  # January 15
                f'{year}0201000000',  # February 1
                f'{year}0301000000',  # March 1
            ]
        else:
            snapshot_dates = []
    
    best_companies = []
    best_count = 0
    
    for date in snapshot_dates:
        try:
            print(f"Trying Wayback Machine archive (snapshot from {date[:8]})...")
            snapshot_url = f'https://web.archive.org/web/{date}/{base_url}'
            response = session.get(snapshot_url, timeout=30)
            if response.status_code == 200 and len(response.text) > 1000:
                companies = parse_glassdoor_page(response.text, year, snapshot_url)
                if companies:
                    print(f"Found {len(companies)} companies from snapshot {date[:8]}")
                    # If we got the expected count, return immediately
                    expected_count = 100 if 2018 <= year <= 2025 else 50
                    if len(companies) >= expected_count:
                        return companies
                    # Otherwise, keep track of the best result
                    if len(companies) > best_count:
                        best_count = len(companies)
                        best_companies = companies
        except Exception as e:
            print(f"Wayback Machine snapshot {date[:8]} failed: {e}")
            continue
    
    # If we found companies (even if not the full count), return them
    if best_companies:
        print(f"Returning best result: {len(best_companies)} companies")
        return best_companies
    
    print("All Wayback Machine attempts failed")
    return []


def scrape_glassdoor(year: int) -> List[str]:
    """
    Scrape company names from Glassdoor's Best Places to Work list for a given year.
    Tries multiple methods including Wayback Machine.
    
    Args:
        year: Year of the list to scrape (2009-2025)
    
    Returns:
        List of company names
    """
    # Validate year - allow up to current year + 1 (in case list is published early)
    current_year = datetime.now().year
    max_year = current_year + 1
    
    if year < 2009 or year > max_year:
        raise ValueError(f"Year must be between 2009 and {max_year}, got {year}")
    
    # Warn if trying to scrape a future year
    if year > current_year:
        print(f"Warning: {year} is in the future. The list may not be available yet.")
    
    # Use Wayback Machine for all years (more reliable, less likely to be blocked)
    print("Using Wayback Machine to access archived page...")
    companies = scrape_from_wayback_machine(year)
    if companies:
        print(f"Successfully scraped {len(companies)} companies from Wayback Machine")
        return companies
    
    return []


def save_companies(companies: List[str], year: int, filename: str = None) -> None:
    """
    Save company names to a text file.
    
    Args:
        companies: List of company names
        year: Year of the list
        filename: Output filename (defaults to data/glassdoor/glassdoor_{year}_companies.txt)
    """
    # Ensure glassdoor directory exists
    os.makedirs(COMPANIES_DIR, exist_ok=True)
    
    if filename is None:
        filename = os.path.join(COMPANIES_DIR, f'glassdoor_{year}_companies.txt')
    with open(filename, 'w', encoding='utf-8') as f:
        for company in companies:
            f.write(f"{company}\n")
    print(f"Saved {len(companies)} companies to {filename}")


def save_companies_json(companies: List[str], year: int, filename: str = None) -> None:
    """
    Save company names to a JSON file.
    
    Args:
        companies: List of company names
        year: Year of the list
        filename: Output filename (defaults to data/glassdoor/glassdoor_{year}_companies.json)
    """
    # Ensure glassdoor directory exists
    os.makedirs(COMPANIES_DIR, exist_ok=True)
    
    if filename is None:
        filename = os.path.join(COMPANIES_DIR, f'glassdoor_{year}_companies.json')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(companies, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(companies)} companies to {filename}")


def main():
    """Main function to run the scraper."""
    # Calculate valid year range
    current_year = datetime.now().year
    max_year = current_year + 1  # Allow current year + 1 in case list is published early
    
    print("Glassdoor Best Places to Work Scraper")
    print("=" * 60)
    print(f"Enter a year (2009-{max_year}), 'all' for all years, or 'quit'/'exit' to stop")
    print("=" * 60)
    
    # Main loop - keep running until user quits
    while True:
        try:
            year_input = input(f"\nEnter the year to scrape (2009-{max_year}) or 'all' for all years (or 'quit'/'exit' to stop): ").strip().lower()
            
            # Check if user wants to quit
            if year_input in ['quit', 'exit', 'q']:
                print("\nExiting scraper. Goodbye!")
                break
            
            # Check if user wants all years
            if year_input == 'all':
                years = list(range(2009, max_year + 1))  # 2009 to max_year inclusive
            else:
                try:
                    year = int(year_input)
                    if 2009 <= year <= max_year:
                        years = [year]
                    else:
                        print(f"Error: Year must be between 2009 and {max_year}. Please try again.")
                        continue
                except ValueError:
                    print(f"Error: '{year_input}' is not a valid year. Please enter a number between 2009 and {max_year}, 'all', or 'quit'.")
                    continue
            
            # Process each year
            for year in years:
                print(f"\n{'='*60}")
                print(f"Starting Glassdoor Best Places to Work {year} scraper...")
                print(f"{'='*60}")
                
                # Check if year is in the future
                if year > current_year:
                    print(f"\nWarning: {year} is in the future. The list may not be published yet.")
                    print("Attempting to scrape anyway...")
                
                try:
                    companies = scrape_glassdoor(year)
                    
                    if companies:
                        print(f"\nFound {len(companies)} companies:")
                        for i, company in enumerate(companies[:10], 1):  # Show first 10
                            print(f"  {i}. {company}")
                        if len(companies) > 10:
                            print(f"  ... and {len(companies) - 10} more")
                        
                        # Save to JSON format
                        save_companies_json(companies, year)
                    else:
                        print(f"\nNo companies found for {year}.")
                        if year > current_year:
                            print(f"This is expected - the {year} list has not been published yet.")
                        else:
                            print("The page structure may have changed.")
                            print("You may need to inspect the page manually and update the selectors.")
                except ValueError as e:
                    print(f"\nError: {e}")
                except Exception as e:
                    print(f"\nUnexpected error for {year}: {e}")
                
                # Add a small delay between years to be polite
                if len(years) > 1 and year != years[-1]:
                    print("\nWaiting 2 seconds before next year...")
                    time.sleep(2)
            
            if len(years) > 1:
                print(f"\n{'='*60}")
                print(f"Completed scraping {len(years)} years!")
                print(f"{'='*60}")
            
            # After completing, the loop will continue and prompt for next input
            
        except KeyboardInterrupt:
            print("\n\nScraper cancelled by user. Exiting...")
            break
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            print("Continuing...")


if __name__ == '__main__':
    main()

