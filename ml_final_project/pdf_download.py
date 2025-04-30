import duckdb
import requests
import typer
from loguru import logger
from tqdm import tqdm
import time

from ml_final_project.config import RAW_DATA_DIR, REPORTS_DIR
from playwright.sync_api import Playwright, sync_playwright

app = typer.Typer()


@app.command()
def main():
    try:
        logger.add(
            str(REPORTS_DIR / "CSC-PDF-download.log"),
            rotation="10 MB",
            retention="10 days",
            level="INFO",
        )

        logger.info("Starting PDF download from CSC job board.")
        dbPath = (
            RAW_DATA_DIR
            / "CivilServiceCommission"
            / "civilservicecommission.duckdb"
        )
        logger.info(f"Connecting to DuckDB database at {dbPath}.")
        db = duckdb.connect(dbPath, read_only=True)

        pdf_ids = db.sql("SELECT action FROM civilservicecommission").pl()
        logger.info(
            f"Found {pdf_ids.shape[0]} PDF IDs to download from the database."
        )

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            for pdf_id in tqdm(pdf_ids["Action"], desc="Downloading PDFs"):
                pdf_path = (
                    RAW_DATA_DIR
                    / "CivilServiceCommission"
                    / "pdfs"
                    / f"{pdf_id}.pdf"
                )
                if not pdf_path.exists():

                    # Download the PDF file
                    url = f"https://csc.gov.ph/career/job/{pdf_id}"

                    page.goto(url)

                    # Use fetch inside the browser context (inherits cookies/auth!)
                    pdf_data = page.evaluate(
                        """
                        async () => {
                            const response = await fetch(window.location.href);
                            const buffer = await response.arrayBuffer();
                            return Array.from(new Uint8Array(buffer));
                        }
                    """
                    )

                    # Convert to binary and save
                    with open(f"{pdf_path}", "wb") as f:
                        f.write(bytes(pdf_data))

                    logger.info(f"Downloaded {pdf_id} to {pdf_path}.")
                    time.sleep(2)

                else:
                    logger.info(
                        f"{pdf_id}.pdf already exists. Skipping download."
                    )

    except KeyboardInterrupt:
        logger.info("Download interrupted by user.")


if __name__ == "__main__":
    app()
