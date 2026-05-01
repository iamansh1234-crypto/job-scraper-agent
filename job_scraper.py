import time
from scrapers.linkedin_scraper import LinkedInScraper
from scrapers.naukri_scraper import NaukriScraper
from utils.logger import logger
from utils.db import db_connection

def run_job_scraper():
    """Main orchestrator function to run all job scrapers"""
    
    logger.info("=" * 50)
    logger.info("Starting Job Scraper Agent")
    logger.info("=" * 50)
    
    # Connect to database
    if not db_connection.connect():
        logger.error("Failed to connect to MongoDB. Exiting...")
        return
    
    # Create indexes
    db_connection.create_indexes()
    
    # Configuration for scraping
    search_configs = [
        {
            'keywords': 'Python Developer',
            'location': 'India',
            'pages': 1
        },
        {
            'keywords': 'Data Scientist',
            'location': 'India',
            'pages': 1
        },
        {
            'keywords': 'Machine Learning Engineer',
            'location': 'India',
            'pages': 1
        }
    ]
    
    try:
        # Scrape LinkedIn
        logger.info("\n" + "=" * 50)
        logger.info("Starting LinkedIn Scraping...")
        logger.info("=" * 50)
        
        linkedin_scraper = LinkedInScraper()
        for config in search_configs:
            try:
                linkedin_scraper.scrape_jobs(
                    keywords=config['keywords'],
                    location=config['location'],
                    pages=config['pages']
                )
                time.sleep(5)  # Wait between searches
            except Exception as e:
                logger.error(f"Error scraping LinkedIn for {config['keywords']}: {e}")
        
        # Scrape Naukri
        logger.info("\n" + "=" * 50)
        logger.info("Starting Naukri Scraping...")
        logger.info("=" * 50)
        
        naukri_scraper = NaukriScraper()
        for config in search_configs:
            try:
                naukri_scraper.scrape_jobs(
                    keywords=config['keywords'],
                    location=config['location'],
                    pages=config['pages']
                )
                time.sleep(5)  # Wait between searches
            except Exception as e:
                logger.error(f"Error scraping Naukri for {config['keywords']}: {e}")
        
        # Get statistics
        logger.info("\n" + "=" * 50)
        logger.info("Scraping Completed - Statistics")
        logger.info("=" * 50)
        
        all_jobs = db_connection.find_all_jobs()
        linkedin_jobs = db_connection.find_all_jobs({'platform': 'LinkedIn'})
        naukri_jobs = db_connection.find_all_jobs({'platform': 'Naukri'})
        
        logger.info(f"Total jobs scraped: {len(all_jobs)}")
        logger.info(f"LinkedIn jobs: {len(linkedin_jobs)}")
        logger.info(f"Naukri jobs: {len(naukri_jobs)}")
        
    except Exception as e:
        logger.error(f"Error in job scraper: {e}")
    
    finally:
        db_connection.close()
        logger.info("\n" + "=" * 50)
        logger.info("Job Scraper Agent Finished")
        logger.info("=" * 50)

if __name__ == "__main__":
    run_job_scraper()