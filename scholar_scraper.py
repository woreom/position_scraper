import os
import re
from typing import List, Dict, Optional
import time
import logging
import urllib.parse
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import unicodedata
import json  # Add if not already imported
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import get_unique_filename, create_folder
from firecrawl_helper import crawl_personal_page  # Update import statement location

def clean_scholar_csv(df: pd.DataFrame) -> pd.DataFrame:
    print(f"Original rows: {len(df)}")
    new_df = df.copy()
    
    # Remove duplicates based on profile_id
    df = df.drop_duplicates(subset='profile_id')
    
    # Clean text fields with consistent normalization
    text_columns = ['name', 'position', 'institute', 'department', 'advisor', 'interests']
    for col in text_columns:
        if col in new_df.columns:
            new_df[col] = df[col].apply(
                lambda x: unicodedata.normalize('NFKD', x).encode('ASCII', 'ignore').decode('ASCII') if isinstance(x, str) else x
            )
    
    # Sort by name
    new_df = new_df.sort_values('name')
    
    # Define final column order with citation metrics
    column_order = [
        'name', 'position', 'institute', 'department',
        'advisor', 'interests', 'email', 'website',
        'profile_id', 'orcid', 'profile_url', 'source_url',
        'funding_likelihood', 'citations', 'h_index', 'i10_index'
    ]
    
    # Ensure all required columns exist, fill with empty strings if missing
    for col in column_order:
        if col not in new_df.columns:
            new_df[col] = ''
    
    # Select and order columns
    new_df = new_df[column_order]
    new_df.columns = new_df.columns.str.upper()
    
    print(f"Cleaned rows: {len(new_df)}")
    return new_df

def normalize_obfuscated_email(text: str) -> Optional[str]:
    """Convert obfuscated email formats to standard format"""
    try:
        # Remove common words that might precede email
        text = text.lower().replace('contact:', '').replace('email:', '').replace('contact info:', '').strip()
        
        # Replace common obfuscation patterns
        patterns = {
            r'\[at\]': '@',
            r'\(at\)': '@',
            r' at ': '@',
            r'\[dot\]': '.',
            r'\(dot\)': '.',
            r' dot ': '.',
            r'\{dot\}': '.',
            r'\s+': '',  # Remove extra spaces
        }
        
        for pattern, replacement in patterns.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
            
        # Validate if result looks like email
        email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
        if re.match(email_pattern, text):
            return text
            
        return None
        
    except Exception:
        return None

def extract_email_from_webpage(url: str, headers: Dict) -> Optional[str]:
    """Extract email from personal webpage content"""
    try:
        if not url or url.startswith(('javascript:', 'mailto:')):
            return None
            
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Remove scripts and styles
        for element in soup(['script', 'style']):
            element.decompose()
            
        text = soup.get_text()
        
        # Look for contact sections
        contact_sections = soup.find_all(['div', 'p', 'span'], 
            string=re.compile(r'contact|email', re.IGNORECASE))
            
        # Check contact sections first
        for section in contact_sections:
            section_text = section.get_text()
            email = normalize_obfuscated_email(section_text)
            if email:
                return email
                
        # Check entire page content
        lines = text.split('\n')
        for line in lines:
            if any(word in line.lower() for word in ['contact', 'email', '@', 'at']):
                email = normalize_obfuscated_email(line)
                if email:
                    return email
                    
        return None
        
    except Exception as e:
        logging.error(f"Error extracting email from {url}: {str(e)}")
        return None

class GoogleScholarScraper:
    def __init__(self):
        self.base_url = "https://scholar.google.com/citations"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        }
        self.researchers = []
        self.logger = logging.getLogger(__name__)
        self.firecrawl_api = None  # Will be initialized when needed
        if not os.getenv('FIRECRAWL_API_KEY') or not os.getenv('OPENAI_API_KEY'):
            self.logger.warning("Missing required API keys")

    def _parse_researchers(self, html: str) -> tuple[List[Dict], str]:
        """Parse researchers and get next page token"""
        soup = BeautifulSoup(html, 'html.parser')
        profiles = []
        next_page_token = ''
        
        # Find pagination section and debug
        pagination = soup.find('div', id='gsc_authors_bottom_pag')
        if pagination:
            # Find next button
            next_button = pagination.find('button', {'aria-label': 'Next'})
            if next_button and next_button.get('onclick'):
                # Handle escaped JavaScript URL
                onclick = next_button['onclick']
                url_part = onclick.split("window.location='")[1].rstrip("'")
                # Replace escaped characters
                url_part = url_part.replace('\\x3d', '=').replace('\\x26', '&')
                
                # Parse the URL parameters
                query_params = urllib.parse.parse_qs(urllib.parse.urlparse(url_part).query)
                if 'after_author' in query_params:
                    next_page_token = query_params['after_author'][0]
                    
                self.logger.debug(f"Next page URL: {url_part}")
                self.logger.debug(f"Next page token: {next_page_token}")
        else:
            self.logger.warning("No pagination section found")

        # Parse researcher profiles
        for profile in soup.find_all('div', class_='gsc_1usr'):
            try:
                name = profile.find('h3', class_='gs_ai_name').text
                profile_url = profile.find('h3', class_='gs_ai_name').find('a')['href']
                profile_id = profile_url.split('user=')[1].split('&')[0]
                
                researcher = {
                    'name': name,
                    'profile_url': f"{self.base_url}?user={profile_id}",
                    'profile_id': profile_id
                }
                profiles.append(researcher)
            except Exception as e:
                self.logger.error(f"Error parsing profile: {e}")
                continue

        return profiles, next_page_token

    def get_profile_details(self, researcher: Dict) -> Dict:
        """Fetch additional details from researcher's profile page"""
        try:
            response = requests.get(researcher['profile_url'], headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Parse basic info
            info_div = soup.find('div', id='gsc_prf_in')
            if info_div:
                # Extract ORCID if present
                orcid = ''
                orcid_text = info_div.text
                if '[ORCID:' in orcid_text:
                    orcid = orcid_text.split('[ORCID:')[1].split(']')[0].strip()
                
            # Find email, homepage and position
            email = ''
            position = ''
            email_div = soup.find('div', class_='gsc_prf_il', string=lambda t: '@' in str(t))
            if email_div:
                email = email_div.text.split('-')[0].strip()
                
            # Get position from the first gsc_prf_il div that doesn't contain an email
            position_div = soup.find('div', class_='gsc_prf_il', string=lambda t: '@' not in str(t))
            if position_div:
                position = position_div.text.strip()
                
            website = ''
            homepage = soup.find('div', class_='gsc_prf_il', id='gsc_prf_ivh')
            if homepage and homepage.find('a'):
                website = homepage.find('a')['href']
                
            # Parse research interests
            interests = []
            interests_div = soup.find('div', id='gsc_prf_int')
            if interests_div:
                for interest in interests_div.find_all('a', class_='gsc_prf_inta'):
                    label = interest['href'].split('mauthors=label:')[1]
                    interests.append(label)
                    
            # Extract citation metrics
            citations = h_index = i10_index = ''
            metrics_table = soup.find('table', id='gsc_rsb_st')
            if metrics_table:
                rows = metrics_table.find_all('tr')
                for row in rows[1:]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        metric_name = cells[0].text.strip().lower()
                        metric_value = cells[1].text.strip()  # All citations value
                        if 'citations' in metric_name:
                            citations = metric_value
                        elif 'h-index' in metric_name:
                            h_index = metric_value
                        elif 'i10-index' in metric_name:
                            i10_index = metric_value
            
            researcher.update({
                'email': email,
                'website': website,
                'orcid': orcid,
                'position': position,
                'interests': ','.join(interests) if interests else '',
                'institute': '',  # Will be updated by Firecrawl
                'department': '',  # Will be updated by Firecrawl
                'advisor': '',     # Will be updated by Firecrawl
                'funding_likelihood': '',
                'citations': citations,
                'h_index': h_index,
                'i10_index': i10_index
            })

            # Use enhanced Firecrawl + OpenAI extraction first
            if researcher.get('website'):
                firecrawl_data = crawl_personal_page(researcher['website'], self.headers)
                if firecrawl_data:
                    # Prioritize Firecrawl/OpenAI extracted email if found
                    if firecrawl_data.get('email'):
                        researcher['email'] = firecrawl_data['email']
                        self.logger.info(f"Found email via Firecrawl/OpenAI for {researcher['name']}")
                    researcher.update(firecrawl_data)
                    self.logger.info(f"Retrieved enhanced data for {researcher['name']}")
            
            # Fallback to traditional email extraction if still no email
            if not researcher.get('email') and researcher.get('website'):
                website_email = extract_email_from_webpage(
                    researcher['website'], 
                    self.headers
                )
                if website_email:
                    researcher['email'] = website_email
                    self.logger.info(f"Found email via fallback method for {researcher['name']}")
            
            time.sleep(3)
            return researcher
        
        except Exception as e:
            self.logger.error(f"Error fetching profile {researcher['profile_id']}: {e}")
            return researcher

    def search_researchers_by_label(self, label: str, pages: int = 5):
        try:
            after_author = ''
            processed_count = 0
            
            for page in tqdm(range(pages), desc="Crawling pages"):
                try:
                    self.logger.info(f"Processing page {page + 1}/{pages}")
                    params = {
                        "view_op": "search_authors",
                        "hl": "en",
                        "mauthors": f"label:{label}",
                        "astart": page * 10
                    }
                    
                    if after_author and page > 0:
                        params["after_author"] = after_author
                    
                    url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
                    self.logger.debug(f"Requesting URL: {url}")
                    
                    response = requests.get(url, headers=self.headers)
                    response.raise_for_status()
                    
                    researchers, next_page_token = self._parse_researchers(response.text)
                    after_author = next_page_token
                    
                    self.logger.info(f"Found {len(researchers)} researchers on page {page + 1}")
                    
                    # Process researcher profiles concurrently
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        future_to_profile = {
                            executor.submit(self.get_profile_details, researcher): researcher
                            for researcher in researchers
                        }
                        for future in as_completed(future_to_profile):
                            profile = future.result()
                            self.researchers.append(profile)
                            processed_count += 1
                            self.logger.info(f"Processed {processed_count} profiles")
                    
                    if not after_author:
                        self.logger.info("No more pages available")
                        break
                        
                    time.sleep(2)
                    
                except Exception as e:
                    self.logger.error(f"Error on page {page + 1}: {e}")
                    self.save_results(label)
                    continue
                    
        except KeyboardInterrupt:
            self.logger.warning("Crawling interrupted by user")
            self.save_results(label)
            raise

    def save_results(self, label: str):
        create_folder(label)
        filename = os.path.join(label, "researchers")
        print(filename)
        filename = get_unique_filename(filename)
        print(filename)
        df = pd.DataFrame(self.researchers)
        clean_scholar_csv(df).to_csv(filename, index=False)
        self.logger.info(f"Saved {len(self.researchers)} profiles to {filename}")

def main(label: str = "meta_learning", pages: int = 5):
    logging.basicConfig(level=logging.INFO)
    
    scraper = GoogleScholarScraper()
    scraper.search_researchers_by_label(label=label, pages=pages)
    scraper.save_results(label)

if __name__ == "__main__":
    
    labels = "large_language_model"
    pages = 500
    main(label=labels, pages=pages)