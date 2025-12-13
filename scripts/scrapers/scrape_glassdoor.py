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
        if len(section_companies) >= 40:
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
    filtered_companies = []
    exclude_terms = {'company reviews', 'reviews', 'company', 'companies', 'employer', 
                     'employers'}  # Generic navigation terms
    
    # Also exclude companies that appear in navigation/footer (not in the ranking)
    navigation_companies = {'target', 'walmart', 'macy', 'home depot', 'ibm', 
                           'microsoft', 'amazon', 'best buy reviews'}  # These appear in nav, not ranking
    
    for company in unique_companies:
        company_lower = company.lower()
        # Skip if it's a generic term
        if company_lower in exclude_terms:
            continue
        # Skip if it's a navigation company (unless it's actually in the ranking)
        if company_lower in navigation_companies:
            # Only skip if we have 50+ companies (meaning we can afford to filter)
            if len(unique_companies) > 50:
                continue
        # Skip if it's too short or too long
        if not (3 <= len(company) <= 80):
            continue
        # Must contain letters
        if not any(c.isalpha() for c in company):
            continue
        filtered_companies.append(company)
    
    # If we have more than 50, try to identify the top 50 by looking for ranking context
    if len(filtered_companies) > 50:
        # Try to find companies that appear in the ranking section specifically
        ranking_companies = []
        for div in soup.find_all(['div', 'section', 'article']):
            text = div.get_text()
            if 'best places to work' in text.lower() and year_str in text:
                # Extract companies from this section
                section_links = div.find_all('a', href=re.compile(r'/Reviews/.*-Reviews'))
                if len(section_links) >= 40:  # Should have many company links
                    for link in section_links:
                        href = link.get('href', '')
                        match = re.search(r'/Reviews/([^/?-]+(?:-[^/?-]+)*?)(?:-Reviews|/|$)', href)
                        if match:
                            company_name = match.group(1)
                            if company_name.lower() not in exclude_terms:
                                company_name = normalize_company_name(company_name)
                                
                                if company_name and company_name not in ranking_companies:
                                    ranking_companies.append(company_name)
                    if len(ranking_companies) >= 50:
                        return ranking_companies[:50]
    
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
    
    # For older years (2009-2015), try Wayback Machine snapshots
    if year <= 2015:
        # Try dates from the year after (when list would be archived)
        next_year = year + 1
        snapshot_dates = [
            f'{next_year}0115000000',  # January 15
            f'{next_year}0201000000',  # February 1
            f'{next_year}0301000000',  # March 1
            f'{year}1201000000',  # December of the year
            f'{next_year}0601000000',  # June
        ]
    else:
        # For recent years, try current year dates
        current_year = datetime.now().year
        if year <= current_year:
            snapshot_dates = [
                f'{year}0115000000',  # January 15
                f'{year}0201000000',  # February 1
                f'{year}0301000000',  # March 1
            ]
    
    for date in snapshot_dates:
        try:
            print(f"Trying Wayback Machine archive (snapshot from {date[:8]})...")
            snapshot_url = f'https://web.archive.org/web/{date}/{base_url}'
            response = session.get(snapshot_url, timeout=30)
            if response.status_code == 200 and len(response.text) > 1000:
                companies = parse_glassdoor_page(response.text, year, snapshot_url)
                if companies:
                    print(f"Found {len(companies)} companies from snapshot {date[:8]}")
                    return companies
        except Exception as e:
            print(f"Wayback Machine snapshot {date[:8]} failed: {e}")
            continue
    
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
    # Validate year
    if year < 2009 or year > 2025:
        raise ValueError(f"Year must be between 2009 and 2025, got {year}")
    
    # For older years (2009-2015), try Wayback Machine first
    if year <= 2015:
        companies = scrape_from_wayback_machine(year)
        if companies:
            print(f"Successfully scraped {len(companies)} companies from Wayback Machine")
            return companies
    
    # Try direct URL (works better for recent years)
    url = f'https://www.glassdoor.com/Award/Best-Places-to-Work-{year}-LST_KQ0,24.htm'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    try:
        print(f"Trying direct URL: {url}")
        time.sleep(2)  # Be polite, add a delay
        response = session.get(url, timeout=30)
        response.raise_for_status()
        companies = parse_glassdoor_page(response.text, year, url)
        if companies:
            print(f"Successfully scraped {len(companies)} companies from direct URL")
            return companies
    except requests.exceptions.RequestException as e:
        print(f"Error fetching direct URL: {e}")
        # If direct URL fails and it's an old year, we already tried Wayback Machine
        if year > 2015:
            # For recent years, try Wayback Machine as fallback
            companies = scrape_from_wayback_machine(year)
            if companies:
                return companies
    except Exception as e:
        print(f"Error parsing direct URL: {e}")
    
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
    os.makedirs(GLASSDOOR_DIR, exist_ok=True)
    
    if filename is None:
        filename = os.path.join(GLASSDOOR_DIR, f'glassdoor_{year}_companies.txt')
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
    os.makedirs(GLASSDOOR_DIR, exist_ok=True)
    
    if filename is None:
        filename = os.path.join(GLASSDOOR_DIR, f'glassdoor_{year}_companies.json')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(companies, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(companies)} companies to {filename}")


def main():
    """Main function to run the scraper."""
    # Always prompt user for year input after program loads
    while True:
        try:
            year_input = input("Enter the year to scrape (2009-2025): ").strip()
            year = int(year_input)
            if 2009 <= year <= 2025:
                break
            else:
                print(f"Error: Year must be between 2009 and 2025. Please try again.")
        except ValueError:
            print(f"Error: '{year_input}' is not a valid year. Please enter a number between 2009 and 2025.")
        except KeyboardInterrupt:
            print("\n\nScraper cancelled by user.")
            return
    
    print(f"Starting Glassdoor Best Places to Work {year} scraper...")
    
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
        print("\nNo companies found. The page structure may have changed.")
        print("You may need to inspect the page manually and update the selectors.")


if __name__ == '__main__':
    main()

