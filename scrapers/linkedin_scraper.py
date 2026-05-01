import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import config
from models.job import Job
from utils.logger import logger
from utils.db import db_connection
from datetime import datetime

class LinkedInScraper:
    """LinkedIn Job Scraper using Selenium headless browser"""
    
    def __init__(self):
        self.driver = None
        self.wait = None
        self.platform = "LinkedIn"
        self.base_url = config.LINKEDIN_BASE_URL
        self.setup_driver()

    def setup_driver(self):
        """Setup Selenium WebDriver with Chrome headless mode"""
        try:
            chrome_options = Options()
            
            if config.HEADLESS_MODE:
                chrome_options.add_argument('--headless')
            
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument(f'user-agent={config.USER_AGENT}')
            chrome_options.add_argument(f'window-size={config.WINDOW_SIZE[0]},{config.WINDOW_SIZE[1]}')
            
            if config.USE_PROXY and config.PROXY_URL:
                chrome_options.add_argument(f'--proxy-server={config.PROXY_URL}')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, config.REQUEST_TIMEOUT)
            
            logger.info("LinkedIn WebDriver initialized successfully")
        except Exception as e:
            logger.error(f"Error setting up WebDriver: {e}")
            raise

    def scrape_jobs(self, keywords, location="", pages=1):
        """
        Scrape jobs from LinkedIn
        
        Args:
            keywords: Job search keywords
            location: Job location
            pages: Number of pages to scrape
        """
        try:
            search_url = f"{self.base_url}?keywords={keywords}"
            if location:
                search_url += f"&location={location}"
            
            logger.info(f"Starting LinkedIn scrape for: {keywords} in {location}")
            
            for page in range(pages):
                try:
                    self.driver.get(search_url + f"&start={page * 25}")
                    time.sleep(config.LINKEDIN_DELAY)
                    
                    # Wait for job listings to load
                    self.wait.until(EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "div.job-card-container")
                    ))
                    
                    # Get page source
                    page_source = self.driver.page_source
                    soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Find all job cards
                    job_cards = soup.find_all('div', class_='job-card-container')
                    logger.info(f"Found {len(job_cards)} jobs on page {page + 1}")
                    
                    for card in job_cards:
                        try:
                            job_data = self.parse_job_card(card)
                            if job_data:
                                self.save_job(job_data)
                        except Exception as e:
                            logger.warning(f"Error parsing job card: {e}")
                            continue
                
                except TimeoutException:
                    logger.warning(f"Timeout loading page {page + 1}")
                    continue
                except Exception as e:
                    logger.error(f"Error scraping page {page + 1}: {e}")
                    continue
            
            logger.info("LinkedIn scraping completed")
        
        except Exception as e:
            logger.error(f"Error in LinkedIn scraper: {e}")
        
        finally:
            self.close()

    def parse_job_card(self, card):
        """Parse job information from a job card"""
        try:
            # Extract job details
            job_title = card.find('h3', class_='job-card-title')
            company = card.find('h4', class_='job-card-company-name')
            location = card.find('span', class_='job-card-location')
            link = card.find('a', class_='job-card-link')
            
            if not all([job_title, company]):
                return None
            
            job_title_text = job_title.get_text(strip=True)
            company_text = company.get_text(strip=True)
            location_text = location.get_text(strip=True) if location else "Not specified"
            application_link = link['href'] if link else ""
            
            # Click on job to get more details
            if link:
                self.driver.execute_script("arguments[0].click();", link)
                time.sleep(config.SCRAPE_DELAY)
                
                # Extract detailed information
                description = self.extract_text_safe("div.show-more-less-html__markup")
                requirements = self.extract_list_safe("ul li")
                benefits = self.extract_text_safe("div.description__job-criteria-item")
                salary = self.extract_text_safe("span.salary")
                posting_date = self.extract_text_safe("span.posted-time-ago__text")
            
            job = Job(
                job_title=job_title_text,
                company=company_text,
                location=location_text,
                salary=salary or "Not specified",
                description=description or "Not available",
                requirements=requirements or [],
                benefits=benefits or [],
                application_link=application_link,
                posting_date=posting_date or datetime.now(),
                platform=self.platform
            )
            
            return job
        
        except Exception as e:
            logger.error(f"Error parsing job card: {e}")
            return None

    def extract_text_safe(self, selector):
        """Safely extract text from a CSS selector"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.get_text(strip=True)
        except NoSuchElementException:
            return ""

    def extract_list_safe(self, selector):
        """Safely extract list items from a CSS selector"""
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
            return [elem.get_text(strip=True) for elem in elements]
        except NoSuchElementException:
            return []

    def save_job(self, job):
        """Save job to MongoDB"""
        try:
            # Check if job already exists
            existing = db_connection.find_job(job.unique_id)
            
            if existing:
                logger.debug(f"Job already exists: {job.job_title} at {job.company}")
            else:
                db_connection.insert_job(job.to_dict())
                logger.info(f"Job saved: {job.job_title} at {job.company}")
        
        except Exception as e:
            logger.error(f"Error saving job: {e}")

    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            logger.info("LinkedIn WebDriver closed")

# Main execution
if __name__ == "__main__":
    db_connection.connect()
    db_connection.create_indexes()
    
    scraper = LinkedInScraper()
    scraper.scrape_jobs(keywords="Python Developer", location="India", pages=2)
    
    db_connection.close()