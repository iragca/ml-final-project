import typer

from ml_final_project.config import REPORTS_DIR, logger
from ml_final_project.scrapers import CSC

app = typer.Typer()


@app.command()
def CSCJobBoard(
    num_pages: int = -1,  # Number of pages to scrape. -1 for all pages.
    headless: bool = False,  # Run in headless mode (no GUI)
    use_duckdb: bool = True,  # Save to DuckDB instead of CSV
):
    try:
        logger.add(
            str(REPORTS_DIR / "CSC-scrape.log"),
            rotation="10 MB",
            retention="10 days",
            level="INFO",
        )
        scraper = CSC()
        scraper.start_scrape(num_pages, headless, use_duckdb)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user.")


if __name__ == "__main__":
    app()
