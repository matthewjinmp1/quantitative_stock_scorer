"""
Scrape company names from Glassdoor's Best Places to Work 2009 list
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import re
import os
from typing import List, Optional

# Get project root directory (2 levels up from this script)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


def scrape_from_wayback_machine() -> List[str]:
    """
    Try to scrape from Wayback Machine archive.
    
    Returns:
        List of company names, or empty list if not found
    """
    # Try Wayback Machine archive URLs
    wayback_urls = [
        'https://web.archive.org/web/20100101000000*/https://www.glassdoor.com/Award/Best-Places-to-Work-2009-LST_KQ0,24.htm',
        'https://web.archive.org/web/2010*/https://www.glassdoor.com/Award/Best-Places-to-Work-2009-LST_KQ0,24.htm',
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://web.archive.org/',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # Try multiple Wayback Machine snapshots from different dates
    snapshot_dates = [
        '20100115000000',  # January 2010
        '20100201000000',  # February 2010
        '20100301000000',  # March 2010
        '20091201000000',  # December 2009
        '20100601000000',  # June 2010
    ]
    
    for date in snapshot_dates:
        try:
            print(f"Trying Wayback Machine archive (snapshot from {date[:8]})...")
            snapshot_url = f'https://web.archive.org/web/{date}/https://www.glassdoor.com/Award/Best-Places-to-Work-2009-LST_KQ0,24.htm'
            response = session.get(snapshot_url, timeout=30)
            if response.status_code == 200 and len(response.text) > 1000:  # Make sure we got actual content
                companies = parse_glassdoor_page(response.text, snapshot_url)
                if companies:
                    print(f"Found {len(companies)} companies from snapshot {date[:8]}")
                    return companies
        except Exception as e:
            print(f"Wayback Machine snapshot {date[:8]} failed: {e}")
            continue
    
    print("All Wayback Machine attempts failed")
    return []


def parse_glassdoor_page(html_content: str, source_url: str = "") -> List[str]:
    """
    Parse company names from Glassdoor HTML content by extracting from link structure.
    
    Args:
        html_content: HTML content to parse
        source_url: URL where content came from (for logging)
    
    Returns:
        List of company names
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    company_names = []
    
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
            'Juniper Networks': 'Juniper Networks',  # Keep as is
        }
        for old, new in name_fixes.items():
            if old in company_name:
                company_name = company_name.replace(old, new)
        return company_name
    
    # Method 1: Extract company names from Review links (most reliable)
    # Company names are in URLs like /Reviews/CompanyName-Reviews
    review_link_companies = []
    seen_hrefs = set()  # Track seen hrefs to avoid duplicates
    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        # Look for patterns like /Reviews/CompanyName-Reviews
        # Also handle variations like /Reviews/CompanyName or /Reviews/Company-Name-Reviews-E123
        # Pattern: /Reviews/CompanyName-Reviews or /Reviews/Company-Name-Reviews-E123
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
    # Look for a section that contains "Best Places to Work 2009" or ranking numbers
    ranking_section = None
    best_section = None
    best_count = 0
    
    for div in soup.find_all(['div', 'section', 'article']):
        text = div.get_text()
        # Check if this section contains ranking-related content
        if 'best places to work' in text.lower() and '2009' in text:
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
    # (not navigation elements, not generic terms)
    filtered_companies = []
    exclude_terms = {'company reviews', 'reviews', 'company', 'companies', 'employer', 
                     'employers'}  # Generic navigation terms
    
    # Also exclude companies that appear in navigation/footer (not in the ranking)
    # These are typically large retailers that appear in "trending" sections
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
    
    # If we have exactly 50, return them
    # If we have more than 50, try to identify the top 50 by looking for ranking context
    if len(filtered_companies) > 50:
        # Try to find companies that appear in the ranking section specifically
        # Look for a section containing "Best Places to Work 2009"
        ranking_companies = []
        for div in soup.find_all(['div', 'section', 'article']):
            text = div.get_text()
            if 'best places to work' in text.lower() and '2009' in text:
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
    
    # Filter candidates aggressively
    filtered_companies = []
    for candidate in all_candidates:
        candidate_lower = candidate.lower().strip()
        
        # Skip if too short or too long
        if not (3 <= len(candidate) <= 80):
            continue
        
        # Skip if it matches exclusion patterns
        if any(pattern in candidate_lower for pattern in exclude_patterns):
            continue
        
        # Skip if it's mostly numbers or symbols
        if not any(c.isalpha() for c in candidate):
            continue
        
        # Skip month abbreviations
        if candidate_lower in ['mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'jan', 'feb']:
            continue
        
        # Skip if it contains common job-related words
        if any(word in candidate_lower for word in [' jobs', 'job ', ' salary', 'salaries', ' reviews', 'review ']):
            continue
        
        # Skip if it looks like a quote (starts/ends with quotes, contains "I'm", "we", etc.)
        if candidate.startswith('"') or candidate.endswith('"') or candidate.startswith("'") or candidate.endswith("'"):
            continue
        if any(phrase in candidate_lower for phrase in ["i'm", "i am", "we ", "my ", "our ", "great place", "excellent", "wonderful", "amazing"]):
            continue
        # Skip if it contains ellipsis (likely a quote/sentence)
        if '...' in candidate or '…' in candidate:
            continue
        # Skip if it looks like a sentence (contains multiple commas or periods in the middle)
        if candidate.count(',') > 2 or (candidate.count('.') > 1 and not candidate.endswith('.')):
            continue
        # Skip common footer/navigation phrases
        if any(phrase in candidate_lower for phrase in ['share via', 'work in hr', 'about us', 'talk to sales', 
                                                         'help center', 'terms of use', 'privacy', 'work with us',
                                                         'available on', 'android app', 'browse by', 'blog', 'employers',
                                                         'community', 'guidelines', 'advertisers', 'developers', 'careers']):
            continue
        
        # Skip if it's too long (likely a sentence/quote, not a company name)
        if len(candidate) > 60:
            continue
        
        # Skip single words that are common UI elements (unless they're well-known companies)
        well_known_single_word_companies = {'google', 'netflix', 'adobe', 'sap', 'intuit', 'fedex', 'cisco', 'usaa', 'ti'}
        if len(candidate.split()) == 1 and candidate_lower not in well_known_single_word_companies:
            # Skip common single words
            if candidate_lower in ['companies', 'company', 'follow', 'star', 'keyword', 'location', 'shares', 'share']:
                continue
        
        # Prefer names that start with capital, but don't require it
        # (some HTML might have lowercase, but we'll capitalize later if needed)
        filtered_companies.append(candidate)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_companies = []
    for company in filtered_companies:
        company_lower = company.lower().strip()
        if company_lower and company_lower not in seen:
            seen.add(company_lower)
            unique_companies.append(company)
    
    # Debug: if we found very few companies, the filtering might be too aggressive
    if len(unique_companies) < 5 and len(all_candidates) > 20:
        print(f"Warning: Filtered {len(all_candidates)} candidates down to {len(unique_companies)} companies.")
        print(f"First 10 candidates were: {all_candidates[:10]}")
    
    return unique_companies


def scrape_glassdoor_2009() -> List[str]:
    """
    Scrape company names from Glassdoor's Best Places to Work 2009 list.
    Tries multiple methods including Wayback Machine.
    
    Returns:
        List of company names
    """
    # Try Wayback Machine first (most likely to work for old pages)
    companies = scrape_from_wayback_machine()
    if companies:
        print(f"Successfully scraped {len(companies)} companies from Wayback Machine")
        return companies
    
    # If Wayback Machine fails, try direct URL with improved headers
    url = 'https://www.glassdoor.com/Award/Best-Places-to-Work-2009-LST_KQ0,24.htm'
    
    # Enhanced headers to mimic a real browser
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
        companies = parse_glassdoor_page(response.text, url)
        if companies:
            print(f"Successfully scraped {len(companies)} companies from direct URL")
            return companies
    except requests.exceptions.RequestException as e:
        print(f"Error fetching direct URL: {e}")
    except Exception as e:
        print(f"Error parsing direct URL: {e}")
    
    return []


def save_companies(companies: List[str], filename: str = None) -> None:
    """
    Save company names to a text file.
    
    Args:
        companies: List of company names
        filename: Output filename (defaults to data/glassdoor_2009_companies.txt)
    """
    if filename is None:
        filename = os.path.join(DATA_DIR, 'glassdoor_2009_companies.txt')
    with open(filename, 'w', encoding='utf-8') as f:
        for company in companies:
            f.write(f"{company}\n")
    print(f"Saved {len(companies)} companies to {filename}")


def save_companies_json(companies: List[str], filename: str = None) -> None:
    """
    Save company names to a JSON file.
    
    Args:
        companies: List of company names
        filename: Output filename (defaults to data/glassdoor_2009_companies.json)
    """
    if filename is None:
        filename = os.path.join(DATA_DIR, 'glassdoor_2009_companies.json')
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(companies, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(companies)} companies to {filename}")


def main():
    """Main function to run the scraper."""
    print("Starting Glassdoor Best Places to Work 2009 scraper...")
    
    companies = scrape_glassdoor_2009()
    
    if companies:
        print(f"\nFound {len(companies)} companies:")
        for i, company in enumerate(companies[:10], 1):  # Show first 10
            print(f"  {i}. {company}")
        if len(companies) > 10:
            print(f"  ... and {len(companies) - 10} more")
        
        # Save to both text and JSON formats
        save_companies(companies)
        save_companies_json(companies)
    else:
        print("\nNo companies found. The page structure may have changed.")
        print("You may need to inspect the page manually and update the selectors.")


if __name__ == '__main__':
    main()

