from pathlib import Path

import typer
from loguru import logger
from tqdm import tqdm

from ml_final_project.config import RAW_DATA_DIR
from ml_final_project.scrapers.CSC import CSC

app = typer.Typer()


@app.command()
def CSCJobBoard(
    num_pages: int = -1,  # Number of pages to scrape. -1 for all pages.
    headless: bool = False,  # Run in headless mode (no GUI)
    use_duckdb: bool = True,  # Save to DuckDB instead of CSV
):
    try:
        scraper = CSC()
        scraper.start_scrape(num_pages, headless, use_duckdb)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user.")


if __name__ == "__main__":
    app()
