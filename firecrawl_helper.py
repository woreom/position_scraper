import os
from typing import Dict
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
from data_extractor import extract_researcher_data  # Changed from relative to absolute import
import time
from collections import deque
from datetime import datetime, timedelta

class RateLimiter:
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window  # in seconds
        self.requests = deque()
    
    def wait_if_needed(self):
        now = datetime.now()
        # Remove requests older than time window
        while self.requests and (now - self.requests[0]) > timedelta(seconds=self.time_window):
            self.requests.popleft()
        
        # If at rate limit, wait until oldest request expires
        if len(self.requests) >= self.max_requests:
            wait_time = (self.requests[0] + timedelta(seconds=self.time_window) - now).total_seconds()
            if wait_time > 0:
                print(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
        
        # Add current request
        self.requests.append(now)

# Global rate limiter instance
rate_limiter = RateLimiter(max_requests=10, time_window=60)

def crawl_personal_page(url: str, headers: Dict) -> Dict:
    """Extract webpage information using Firecrawl API and OpenAI"""
    try:
        load_dotenv()
        api_key = os.getenv('FIRECRAWL_API_KEY')
        if not api_key:
            print("Warning: FIRECRAWL_API_KEY not found in environment variables")
            return {}
            
        app = FirecrawlApp(api_key=api_key)
        
        # Wait for rate limit if needed
        rate_limiter.wait_if_needed()
        
        params = {
            'extract': {
                'schema': {
                    'type': 'object',
                    'properties': {
                        'contact': {
                            'selector': '.contact, #contact, .contact-email, .email'
                        },
                        'mainContent': {
                            'selector': 'main, article, .main-content'
                        }
                    }
                },
                'options': {
                    'textOnly': True,
                    'maxTextLength': 10000//3
                }
            }
        }
        
        try:
            page_content = app.scrape_url(url=url, params=params)
            if isinstance(page_content, dict):
                # Combine content from different sections
                combined_content = []
                if 'contact' in page_content:
                    combined_content.append(str(page_content['contact']))
                if 'mainContent' in page_content:
                    combined_content.append(str(page_content['mainContent']))
                page_content = ' '.join(combined_content)
        except Exception as e:
            # Wait again before retry if needed
            rate_limiter.wait_if_needed()
            # Fallback to basic scraping if structured extraction fails
            page_content = app.scrape_url(url=url)
            
        # Extract structured data using OpenAI
        extracted_data = extract_researcher_data(page_content)
        
        # Ensure consistent field names including funding_likelihood
        result = {
            'position': extracted_data.get('position', ''),
            'institute': extracted_data.get('institute', ''),
            'department': extracted_data.get('department', ''),
            'advisor': extracted_data.get('advisor', ''),
            'interests': extracted_data.get('interests', ''),
            'email': extracted_data.get('email', ''),
            'funding_likelihood': extracted_data.get('funding_likelihood', 'Medium'),  # Default to Medium if not determined
            'source_url': url
        }
        
        # Remove empty values but keep funding_likelihood
        result = {k: v for k, v in result.items() if v or k == 'funding_likelihood'}
        
        time.sleep(1)  # Rate limiting
        return result
        
    except Exception as e:
        print(f"Firecrawl error for {url}: {str(e)}")
        return {}
