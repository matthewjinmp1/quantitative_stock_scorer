"""
Scrape company names from Glassdoor's Best Places to Work 2009 list
"""
import requests
from bs4 import BeautifulSoup
import json
import time
import re
from typing import List, Optional


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
    Parse company names from Glassdoor HTML content.
    
    Args:
        html_content: HTML content to parse
        source_url: URL where content came from (for logging)
    
    Returns:
        List of company names
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    company_names = []
    
    # Extensive list of patterns to exclude (navigation, UI elements, job titles, etc.)
    exclude_patterns = [
        'employer resources', 'additional resources', 'how the top', 'why didn\'t',
        'jobs', 'job', 'salary', 'salaries', 'benefits', 'glassdoor', 'best places',
        'award', 'click', 'view', 'more', 'less', 'sign in', 'register', 'menu',
        'search', 'explore', 'company culture', 'tips', 'tools', 'near you',
        'by city', 'top job', 'highest paying', 'pay raises', 'types of companies',
        'according to', 'hiring now', 'work-life balance', 'paid time off',
        'clerical', 'administrative', 'assistant', 'human resources', 'project manager',
        'receptionist', 'customer service', 'representative', 'attorney', 'graphic designer',
        'executive', 'office', 'medical', 'mar', 'apr', 'may', 'jun', 'jul', 'aug',
        'sep', 'oct', 'nov', 'dec', 'jan', 'feb', 'chicago', 'new york', 'dallas',
        'houston', 'los angeles', 'atlanta', 'america', 'snapchat', 'facebook',
        'reviews', 'overview', 'interview', 'interviews',
        # Wayback Machine specific
        'about this capture', 'collected by', 'organization', 'internet archive',
        'the goal is to', 'collection', 'gdelt', 'the gdelt project', 'timestamps',
        'account settings', 'notifications', 'for employers', 'get free employer account',
        'employer branding', 'recruiting blog', 'contact sales', 'keyword', 'location',
        'choose a list', 'highest rated ceos', 'career opportunities', 'company trends',
        'lists', 'trends', 'trends by industry', 'trends by location', 'want to recruit',
        'find out how', 'trends faq', 'free employer account', 'press center',
        'choose a location', 'united states', 'choose a year', 'shares', 'share on',
        'follow', 'star', 'highest rated ceos:'
    ]
    
    # Method 1: Look for numbered lists or ranking patterns
    ranking_patterns = []
    
    # First, try to find the main content area (often in a div with id/class containing "content", "main", "list", "ranking")
    main_content = None
    for selector in ['#content', '#main', '.content', '.main', '[class*="list"]', '[class*="ranking"]', '[id*="list"]']:
        main_content = soup.select_one(selector)
        if main_content:
            break
    
    # If we found main content, search within it; otherwise search the whole page
    search_area = main_content if main_content else soup
    
    # Look for ordered lists (numbered lists)
    for ol in search_area.find_all('ol'):
        for li in ol.find_all('li', recursive=False):
            text = li.get_text(strip=True)
            # Remove leading numbers and punctuation
            text = text.lstrip('0123456789.()[]- ').strip()
            # Split by newlines and take the first line (company name, not quote)
            text = text.split('\n')[0].split('.')[0].strip()
            if text and 3 <= len(text) <= 80:
                ranking_patterns.append(text)
    
    # Method 2: Look for text patterns like "1. Company Name" or "#1 Company Name"
    # Search for text nodes that start with numbers followed by company names
    text_content = search_area.get_text()
    # Look for patterns like "1. Company", "#1 Company", "1) Company"
    number_company_pattern = re.findall(r'(?:^|\n)\s*(?:\d+\.|#\d+|\(\d+\))\s+([A-Z][A-Za-z0-9\s&.,-]{2,50})', text_content)
    ranking_patterns.extend(number_company_pattern)
    
    # Method 3: Look for table rows that might contain rankings
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 2:
                # Skip header rows
                cell_text = ' '.join([cell.get_text(strip=True).lower() for cell in cells[:3]])
                if any(header in cell_text for header in ['rank', 'company', 'name', 'rating', 'header']):
                    continue
                # Get text from second or third cell (usually company name)
                for cell in cells[1:3]:
                    text = cell.get_text(strip=True)
                    text = text.lstrip('0123456789.()[]- ').strip()
                    # Take first line only
                    text = text.split('\n')[0].split('|')[0].strip()
                    if text and 3 <= len(text) <= 80:
                        ranking_patterns.append(text)
    
    # Method 3: Look for headings (h1, h2, h3) that might be company names
    # Company names in rankings are often in headings
    heading_companies = []
    for heading in search_area.find_all(['h1', 'h2', 'h3', 'h4']):
        text = heading.get_text(strip=True)
        # Remove common prefixes like "Rank #", "#1", etc.
        text = text.lstrip('0123456789.#()[]- ').strip()
        # Take only the first line (company name, not subtitle)
        text = text.split('\n')[0].split('|')[0].strip()
        if text and 3 <= len(text) <= 80:
            heading_companies.append(text)
    
    # Method 4: Look for links that might be company profiles
    company_links = []
    for link in search_area.find_all('a', href=True):
        href = link.get('href', '')
        text = link.get_text(strip=True)
        # Check if link looks like a company profile link
        if ('/Overview' in href or '/Reviews' in href or '/company' in href.lower() or 
            '/Employer' in href) and text:
            # Take only the first line
            text = text.split('\n')[0].strip()
            if 3 <= len(text) <= 80:
                company_links.append(text)
    
    # Method 5: Extract all text from main content and look for company-like patterns
    # Get all text nodes and look for capitalized words/phrases that look like company names
    if main_content:
        # Get all text, split by lines
        all_text_lines = main_content.get_text(separator='\n').split('\n')
        for line in all_text_lines:
            line = line.strip()
            # Look for lines that are proper nouns (start with capital, reasonable length)
            if line and 3 <= len(line) <= 60:
                # Must start with capital letter
                if line[0].isupper():
                    # Skip if it's clearly not a company (contains common UI words)
                    if not any(skip in line.lower() for skip in ['click', 'view', 'more', 'less', 'sign', 'login', 'register']):
                        # Skip if it's a date or number
                        if not re.match(r'^\d+', line):
                            ranking_patterns.append(line)
    
    # Method 6: Look for structured data (JSON-LD)
    json_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict):
                if 'itemListElement' in data:
                    for item in data.get('itemListElement', []):
                        if isinstance(item, dict):
                            name = item.get('name') or (item.get('item', {}) if isinstance(item.get('item'), dict) else {}).get('name')
                            if name and 3 <= len(name) <= 80:
                                company_names.append(name)
        except (json.JSONDecodeError, AttributeError, TypeError):
            continue
    
    # Combine all potential company names
    all_candidates = ranking_patterns + heading_companies + company_links + company_names
    
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


def save_companies(companies: List[str], filename: str = 'glassdoor_2009_companies.txt') -> None:
    """
    Save company names to a text file.
    
    Args:
        companies: List of company names
        filename: Output filename
    """
    with open(filename, 'w', encoding='utf-8') as f:
        for company in companies:
            f.write(f"{company}\n")
    print(f"Saved {len(companies)} companies to {filename}")


def save_companies_json(companies: List[str], filename: str = 'glassdoor_2009_companies.json') -> None:
    """
    Save company names to a JSON file.
    
    Args:
        companies: List of company names
        filename: Output filename
    """
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

