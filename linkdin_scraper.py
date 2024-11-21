from calendar import c
import logging
import os
import json
from datetime import datetime
from socket import timeout
import time
from typing import Dict, List
import random

import pandas as pd

from urllib.parse import urlencode
from playwright.sync_api import sync_playwright

from dotenv import load_dotenv

from utils import get_unique_filename, create_folder

# Clean data
def clean_text(text):
    if pd.isna(text):
        return text
    return ' '.join(text.strip().replace('\n', ' ').split())

def get_clean_table(jobs):
    # Clean columns with basic text cleaning
    jobs['title'] = jobs['title'].apply(clean_text)
    jobs['company'] = jobs['company'].apply(clean_text)
    jobs['location'] = jobs['location'].apply(clean_text)
    jobs['metadata'] = jobs['metadata'].apply(clean_text)

    # Remove "with verification" from titles
    jobs['title'] = jobs['title'].str.replace(r'\s*with verification$', '', regex=True)

    # Clean job titles that are duplicated
    jobs['title'] = jobs['title'].str.replace(r'(\w+)\1', r'\1', regex=True)

    # Extract city and state from location
    jobs['location'] = jobs['location'].str.replace(r'\s*\([^)]*\)', '', regex=True)
    
    # Split location into city and state
    location_split = jobs['location'].str.extract(r'(.*?),\s*(.*?)(?:\s|$)')
    jobs['city'] = location_split[0]
    jobs['state'] = location_split[1]

    # Split metadata into separate columns if it contains multiple pieces of info
    if 'metadata' in jobs.columns:
        # Extract salary range if present
        salary_pattern = r'\$(\d+(?:,\d+)?(?:\.\d+)?[KM]?)(?:/\w+)?\s*-\s*\$?(\d+(?:,\d+)?(?:\.\d+)?[KM]?)(?:/\w+)?'
        salary_split = jobs['metadata'].str.extract(salary_pattern)
        jobs['salary_min'] = salary_split[0]
        jobs['salary_max'] = salary_split[1]

        # Extract job type (Full-time, Contract, etc.)
        jobs['job_type'] = jobs['metadata'].str.extract(r'(Full-time|Part-time|Contract|Internship)')

        # Extract experience level
        jobs['experience_level'] = jobs['metadata'].str.extract(r'(Entry level|Associate|Mid-Senior level|Executive)')

        # Extract work model (Remote, Hybrid, On-site)
        jobs['work_model'] = jobs['metadata'].str.extract(r'(Remote|Hybrid|On-site)')

    # Clean up description HTML and excessive whitespace
    if 'description' in jobs.columns:
        jobs['description'] = jobs['description'].str.replace(r'<[^>]+>', '', regex=True)
        jobs['description'] = jobs['description'].apply(clean_text)
        
    # Extract skills from description if available
    if 'skills' in jobs.columns:
        jobs['skills'] = jobs['skills'].apply(clean_text)
        # Remove common filler words
        jobs['skills'] = jobs['skills'].str.replace(r'\b(and|or|the|with|in|of|to|for)\b', '', regex=True)

    # Drop duplicates based on link
    jobs = jobs.drop_duplicates(subset=['link'], keep='first')

    # Remove any rows where essential fields are empty
    jobs = jobs.dropna(subset=['title', 'company'])

    # Capitalize all column names
    jobs.columns = jobs.columns.str.upper()

    # Set a specific column order for better readability
    desired_columns = [
        'TITLE', 'COMPANY', 'CITY', 'STATE', 'SALARY_MIN', 'SALARY_MAX',
        'JOB_TYPE', 'WORK_MODEL', 'EXPERIENCE_LEVEL', 'SKILLS',
        'DESCRIPTION', 'LINK', 'METADATA'
    ]
    jobs = jobs.reindex(columns=[col for col in desired_columns if col in jobs.columns])

    return jobs

def scroll_down_jobs_list(page, container_selector, scroll_steps=5, delay=1000):
    """
    Scrolls down a specific container element on the page.

    Args:
        page: The Playwright page object.
        container_selector: The CSS selector of the container to scroll.
        scroll_steps: Number of times to scroll down.
        delay: Delay in milliseconds between scrolls.
    """
    for _ in range(scroll_steps):
        page.evaluate(f"""
            const container = document.querySelector('{container_selector}');
            if (container) {{
                container.scrollBy(0, container.clientHeight);
            }}
        """)
        page.wait_for_timeout(delay)  # Wait to allow new items to load

class LinkedInJobCrawler:
    def __init__(self):
        load_dotenv()
        self.email = os.getenv('LINKEDIN_EMAIL')
        self.password = os.getenv('LINKEDIN_PASSWORD')
        self.jobs = []
        self.logger = logging.getLogger(__name__)

    def _setup_browser(self, p):
        """Set up the browser with stealth mode and persistent context."""
        browser_args = [
            '--disable-blink-features=AutomationControlled', 
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-site-isolation-trials',
            '--disable-features=BlockInsecurePrivateNetworkRequests'
        ]
        
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36', 
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        ]

        # Create user data directory if it doesn't exist
        user_data_dir = "./chrome_data"
        if not os.path.exists(user_data_dir):
            os.makedirs(user_data_dir)

        browser = p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=os.getenv('BROWSER_HEADLESS', 'true').lower() == 'true',
            args=browser_args,
            user_agent=random.choice(user_agents),
            viewport={'width': 1024, 'height': 768},
            ignore_https_errors=True,
            permissions=['geolocation']
        )
        
        return browser, browser.new_page()

    def _login(self, page):
        """Check login status and only login if needed."""
        try:
            # Go to LinkedIn feed first to check login status
            page.goto('https://www.linkedin.com/feed', timeout=60000)
            
            # If we're already logged in, we'll be on the feed page
            if page.url.startswith('https://www.linkedin.com/feed'):
                self.logger.info("Already logged in")
                return
                
            # If not on feed, we need to login
            page.goto('https://www.linkedin.com/login', timeout=60000)
            page.fill('input[name="session_key"]', self.email)
            page.fill('input[name="session_password"]', self.password)
            with page.expect_navigation(timeout=60000):
                page.click('button[type="submit"]')
                
            if 'checkpoint' in page.url or 'challenge' in page.url:
                self.logger.warning("Verification page detected")
                page.wait_for_selector('.feed-shared-update-v2', timeout=600000)
            else:
                page.wait_for_selector('.feed-shared-update-v2', timeout=10000)
                
            if not page.url.startswith('https://www.linkedin.com/feed'):
                raise Exception("Login failed - unexpected redirect")
            self.logger.info("Login successful")
            
        except Exception as e:
            self.logger.error(f"Login failed: {e}")
            raise

    def run(self, search_params: Dict, max_pages: int = 3):
        """Run crawler with improved page handling for multiple pages."""
        browser = None
        with sync_playwright() as p:
            try:
                browser, page = self._setup_browser(p)
                self._login(page)
                page.wait_for_timeout(1000)
                search_url = self._build_search_url(search_params)
                self.logger.info(f"Navigating to: {search_url}")
                page.goto(search_url, timeout=60000)
                
                selectors = [
                    '.jobs-search-results',
                    '.jobs-search-results-list',
                    '[data-job-id]',
                    '.jobs-search__job-details'
                ]
                for selector in selectors:
                    try:
                        page.wait_for_selector(selector, timeout=10000)
                        self.logger.info(f"Found selector: {selector}")
                        break
                    except Exception:
                        continue
                if not any(s in page.url for s in ['/jobs/search', '/jobs/collections']):
                    raise Exception("Failed to reach jobs search page")

                current_page = 1
                while current_page <= max_pages:
                    self.logger.info(f"Scraping page {current_page} of {max_pages}")
                    
                    container_selector = ".jobs-search-results-list"
                    scroll_down_jobs_list(page, container_selector, scroll_steps=10, delay=1000)

                    job_cards = page.query_selector_all('.job-card-container')
                    self.logger.info(f"Found {len(job_cards)} job cards on page {current_page}")

                    for index, job_card in enumerate(job_cards):
                        self.logger.info(f"Processing job card {index + 1} of {len(job_cards)}")
                        job_details = self._click_job_card_and_extract(page, job_card)
                        if job_details:
                            self.jobs.append(job_details)
                            time.sleep(2)  # Mimic human interaction

                    if current_page < max_pages and not self._navigate_next_page(page):
                        self.logger.info("No more pages available")
                        break
                    
                    current_page += 1
                    page.wait_for_timeout(100)  # Wait between pages

                label = search_params.get('keywords', '')
                self.save_results(label)
            except Exception as e:
                self.logger.error(f"Crawler failed: {e}")
                raise
            finally:
                if browser:
                    browser.close()

    def _build_search_url(self, params: Dict) -> str:
        """Build search URL with parameters."""
        base_url = "https://www.linkedin.com/jobs/search/?"
        query_params = {
            'keywords': params.get('keywords', ''),
            'location': params.get('location', ''),
            'f_TPR': params.get('timespan', 'r604800'),
            'f_E': ','.join(params.get('experience', [])),
            'f_JT': ','.join(params.get('job_type', []))
        }
        return base_url + urlencode(query_params)

    def _get_job_cards(self, page):
        selectors = ['.job-card-container', '[data-job-id]']
        job_cards = []
        for selector in selectors:
            try:
                page.wait_for_selector(selector, timeout=100, state='visible')
                cards = page.query_selector_all(selector)
                if cards:
                    self.logger.info(f"Found {len(cards)} job cards using selector: {selector}")
                    job_cards.extend(cards)
            except Exception as e:
                self.logger.warning(f"Selector {selector} failed: {e}")
        if not job_cards:
            raise Exception("No job cards found after checking selectors")
        return job_cards
    
    def _extract_job_details(self, page):
        """Extract job details from the job detail panel."""
        try:
            title = self._get_text(page, '.job-details-jobs-unified-top-card__job-title')
            company = self._get_text(page, '.job-details-jobs-unified-top-card__company-name')
            location = self._get_text(page, '.job-details-jobs-unified-top-card__primary-description-container')
            metadata = self._get_text(page, '.job-details-jobs-unified-top-card__job-insight')
            skills = self._get_text(page, '.job-details-preferences-and-skills')
            about_the_job = self._get_text(page, '.jobs-description__content')
            link = self._get_attribute(page, '.job-details-jobs-unified-top-card__job-title a', 'href')
            link = f"https://www.linkedin.com{link}" if link and not link.startswith("http") else link
            # link = self._get_apply_link(page)

            # self.logger.info(f"Job Title: {title}, Company: {company}, Location: {location}")
            # self.logger.info(f"About the Job: {about_the_job[:100]}...")  # Log the first 100 characters of the description

            return {
                'title': title,
                'company': company,
                'location': location,
                'metadata': metadata,
                'skills': skills,
                'description': about_the_job,
                'link': link,
            }
        except Exception as e:
            self.logger.error(f"Failed to extract job details: {e}")
            return None
        
    def _click_job_card_and_extract(self, page, job_card):
        """Click on a job card and extract its details, then attempt Easy Apply if available."""
        try:
            job_card.scroll_into_view_if_needed()
            job_card.click()
            page.wait_for_selector('.jobs-description__container', timeout=3000)  # Wait for job detail panel

            # Extract job details
            job_details = self._extract_job_details(page)

            # Check for Easy Apply and handle it
            if page.query_selector('button[aria-label^="Easy Apply"]'):
                self.logger.info(f"Attempting Easy Apply for job: {job_details['title']}")
                result = self.easy_apply(page)
                self.logger.info(f"Easy Apply result: {result}")

            return job_details
        except Exception as e:
            self.logger.error(f"Failed to process job card: {e}")
            return None

    # easy apply not working 
    def easy_apply(self, page):
        """
        Automates the LinkedIn Easy Apply process using patterns from LinkedinEasyApply
        """
        try:
            # Click Easy Apply button
            easy_apply_button = page.query_selector('.jobs-apply-button--top-card button[aria-label^="Easy Apply"]')
            if not easy_apply_button:
                return "Easy Apply button not available"
            
            easy_apply_button.click()
            page.wait_for_selector('.jobs-easy-apply-modal', timeout=5000)

            max_steps = 20  # Maximum number of form steps to prevent infinite loops
            step_count = 0
            
            while step_count < max_steps:
                try:
                    # Wait for form elements to load
                    page.wait_for_selector('.jobs-easy-apply-form-section__grouping', timeout=5000)
                except Exception:
                    # If no form elements found, we might be done or there's an error
                    break

                step_count += 1
                # Handle contact info
                contact_info = page.query_selector_all('.jobs-easy-apply-form-section__grouping')
                for info in contact_info:
                    text = info.text_content().lower()
                    if 'phone number' in text:
                        country_code = info.query_selector('select[id*="phoneNumber"][id*="country"]')
                        if country_code:
                            country_code.select_option(os.getenv('PHONE_COUNTRY_CODE', '+1'))
                        
                        phone_field = info.query_selector('input[id*="phoneNumber"][id*="nationalNumber"]')
                        if phone_field:
                            phone_field.fill(os.getenv('LINKEDIN_PHONE', ''))

                # Handle file uploads
                upload_buttons = page.query_selector_all('input[type="file"]')
                for upload in upload_buttons:
                    upload_type = upload.evaluate('el => el.parentElement.previousElementSibling.textContent').lower()
                    if 'resume' in upload_type:
                        upload.set_input_files(os.getenv('RESUME_PATH', ''))
                    elif 'cover' in upload_type and os.getenv('COVER_LETTER_PATH'):
                        upload.set_input_files(os.getenv('COVER_LETTER_PATH'))

                # Handle additional questions
                questions = page.query_selector_all('.jobs-easy-apply-form-element')
                for question in questions:
                    # Handle radio buttons
                    radios = question.query_selector_all('.fb-text-selectable__option')
                    if radios:
                        question_text = question.text_content().lower()
                        if any(keyword in question_text for keyword in ['authorized', 'clearance', 'citizenship']):
                            radios[-1].click()  # Select last option
                        continue

                    # Handle dropdowns
                    dropdown = question.query_selector('select')
                    if dropdown:
                        options = dropdown.query_selector_all('option')
                        if options and len(options) > 1:
                            dropdown.select_option(index=1)  # Select first non-empty option
                # Find and click next/submit button
                next_button = page.query_selector('.artdeco-button--primary')
                if not next_button:
                    return "No next button found - application may be incomplete"
                    
                button_text = next_button.text_content().lower()
                
                # Check for discard button which may indicate an error
                if page.query_selector('button[aria-label="Dismiss"]'):
                    return "Application failed - encountered error dialog"
                
                if 'submit' in button_text:
                    # Unfollow company if checkbox exists
                    follow_checkbox = page.query_selector('label[for*="follow-company"]')
                    if follow_checkbox:
                        follow_checkbox.click()
                        
                next_button.click()
                page.wait_for_timeout(2000)  # Increased timeout for better reliability
                if 'submit' in button_text:
                    # Unfollow company if checkbox exists
                    follow_checkbox = page.query_selector('label[for*="follow-company"]')
                    if follow_checkbox:
                        follow_checkbox.click()
                        
                next_button.click()
                page.wait_for_timeout(1000)

                # Check for success message
                if page.query_selector('.jobs-apply-success'):
                    return "Application submitted successfully"

            return "Application process completed"

        except Exception as e:
            self.logger.error(f"Easy Apply failed: {e}")
            return f"Error during Easy Apply: {e}"

    def _get_text(self, element, selector: str) -> str:
        """Safely extract text content."""
        el = element.query_selector(selector)
        return el.text_content().strip() if el else None

    def _get_attribute(self, element, selector: str, attr: str) -> str:
        """Safely extract attribute value."""
        el = element.query_selector(selector)
        return el.get_attribute(attr).strip() if el else None

    def save_results(self, label: str = "jobs"):
        output_folder = create_folder(label)
        filename = os.path.join(output_folder, "positions")
        filename = get_unique_filename(filename)
        df = pd.DataFrame(self.jobs)
        df = get_clean_table(df)
        df.to_csv(filename, index=False)
        self.logger.info(f"Saved {len(self.jobs)} positions to {filename}")

    def _navigate_next_page(self, page):
        """Navigate to the next page of job listings."""
        try:
            current_page = page.query_selector('.artdeco-pagination__indicator--number.active')
            if not current_page:
                return False
                
            next_button = current_page.evaluate_handle('node => node.nextElementSibling')
            if not next_button:
                return False

            next_button_element = next_button.as_element()
            if not next_button_element:
                return False

            next_button_element.click()
            page.wait_for_timeout(1000)  # Wait for new content to load
            return True

        except Exception as e:
            self.logger.error(f"Failed to navigate to next page: {e}")
            return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    crawler = LinkedInJobCrawler()
    max_pages = 10
    search_params = {
        'keywords': 'Software Engineer',
        'location': 'California, United States',
        'job_type': ['F', 'C'],
        'experience': ['2', '3'],
        'timespan': 'r604800'
    }
    
    crawler.run(search_params, max_pages = 10)