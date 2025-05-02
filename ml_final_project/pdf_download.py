import duckdb
import typer
from loguru import logger
from tqdm import tqdm
import time

from ml_final_project.config import RAW_DATA_DIR, REPORTS_DIR
from playwright.sync_api import sync_playwright

app = typer.Typer()


@app.command()
def main():
    try:
        logger.add(
            str(REPORTS_DIR / "logs" / "CSC-PDF-download.log"),
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
        PDF_SAVE_PATH = RAW_DATA_DIR / "CivilServiceCommission" / "pdfs"

        pdf_ids = set(
            db.sql("SELECT action FROM civilservicecommission").pl().unique().to_series()
        )

        existing_pdfs = PDF_SAVE_PATH.iterdir()
        existing_pdfs = set([pdf.stem for pdf in existing_pdfs])

        logger.info(f"{len(pdf_ids)} PDF IDs found in the database. Identifying new ones to download...")
        pdf_ids = pdf_ids - existing_pdfs

        logger.info(
            f"Found {len(pdf_ids)} PDF IDs to download from the database."
        )

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=False)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()

            for pdf_id in tqdm(pdf_ids, desc="Downloading PDFs"):
                pdf_path = PDF_SAVE_PATH / f"{pdf_id}.pdf"
                if not pdf_path.exists():

                    # Download the PDF file
                    url = f"https://csc.gov.ph/career/job/{pdf_id}"

                    page.goto(url)
                    page.wait_for_load_state("networkidle")

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
                    # time.sleep(2)

                else:
                    logger.info(
                        f"{pdf_id}.pdf already exists. Skipping download."
                    )

    except KeyboardInterrupt:
        logger.info("Download interrupted by user.")


if __name__ == "__main__":
    app()
