import time
from itertools import count

import numpy as np
from loguru import logger
from playwright.sync_api import Playwright, sync_playwright
from tqdm import tqdm

from ml_final_project.scrapers import BaseScraper


class CSC(BaseScraper):
    """Scraper for the Civil Service Commission (CSC) Careers job board.
    URL - https://csc.gov.ph/career/
    This website uses Load-More pagination, which means that the page loads more job listings as you click "next"
    """

    def __init__(self):
        super().__init__(
            name="CivilServiceCommission", url="https://csc.gov.ph/career/"
        )

        self.COUNTER = count(1)

    def _scrape(
        self, pw: Playwright, num_pages, headless: bool, use_duckdb: bool
    ):
        """Scrape the job listings from the CSC Careers website.
        Args:
            pw (Playwright): Playwright instance.
            num_pages (int): Number of pages to scrape. -1 for all pages.
        Returns:
            pd.DataFrame: DataFrame containing the job listings.
        """
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()

        save = self.save_to_duckdb if use_duckdb else self.save_to_csv

        page.goto(self.URL)
        page.wait_for_load_state("domcontentloaded")

        # Select 100 number of jobs to display per page
        page.select_option("select[name='jobs_length']", "100")
        time.sleep(10)

        random_numbers = np.random.standard_gamma(a=15, size=15000)

        def scrape_page():
            page.wait_for_load_state("domcontentloaded")

            save(page)

            next_button = page.locator("a.paginate_button.next")

            # Get class attribute
            class_attr = next_button.get_attribute("class")

            if class_attr and "disabled" in class_attr:
                logger.info("No more pages to scrape.")
                exit(0)

            next_button.click()
            logger.info(f"Scraped page {next(self.COUNTER)}")
            time.sleep(np.random.choice(random_numbers))

        if num_pages == -1:
            while True:
                try:
                    scrape_page()
                except Exception as e:
                    logger.error("Error scraping page.")
                    logger.error(e)
                    break
        else:
            try:
                for i in tqdm(range(0, num_pages)):
                    scrape_page()
            except Exception as e:
                logger.error("Error scraping page.")
                logger.error(e)

        context.close()
        browser.close()

    def start_scrape(self, num_pages, headless: bool, use_duckdb: bool):
        """Start the scraping process.
        Args:
            num_pages (int): Number of pages to scrape. -1 for all pages.
        Returns:
            pd.DataFrame: DataFrame containing the job listings.
        """

        with sync_playwright() as pw:
            self._scrape(pw, num_pages, headless, use_duckdb)
