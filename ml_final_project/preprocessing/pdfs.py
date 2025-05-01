from itertools import count

import duckdb
from tqdm import tqdm

from ml_final_project.config import (
    INTERIM_DATA_DIR,
    RAW_DATA_DIR,
    REPORTS_DIR,
    logger,
)
from ml_final_project.preprocessing.PDFParser import PDFParser


def main():
    dbPath = (
        INTERIM_DATA_DIR
        / "CivilServiceCommission"
        / "civilservicecommission_pdfs.duckdb"
    )
    pdfsPath = RAW_DATA_DIR / "CivilServiceCommission" / "pdfs"

    if not dbPath.parent.exists():
        dbPath.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory for the database: {dbPath.parent}")

    logger.info(f"Connecting to DuckDB database at {dbPath}.")
    db = duckdb.connect(dbPath)

    #  Create the table if it doesn't exist
    db.sql(
        """
CREATE TABLE IF NOT EXISTS civilservicecommission_pdfs (
        jobId INT PRIMARY KEY,
        Agency VARCHAR,
        PlaceOfAssignment VARCHAR,
        PositionTitle VARCHAR,
        PlantillaNo VARCHAR,
        SalaryGrade VARCHAR,
        MonthlySalary INT,
        Eligibility VARCHAR,
        Education VARCHAR,
        Training VARCHAR,
        Experience VARCHAR,
        Competency VARCHAR
    )
        """
    )

    insert_query = """
    INSERT INTO civilservicecommission_pdfs (
        jobId, Agency, PlaceOfAssignment, PositionTitle, PlantillaNo, SalaryGrade,
        MonthlySalary, Eligibility, Education, Training, Experience, Competency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    logger.info("Processing and inserting data into the database.")
    pdf_files = [f for f in pdfsPath.iterdir() if f.suffix == ".pdf"]

    parser = PDFParser()
    counter = count()

    try:
        for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
            parsed_pdf = parser.parse(pdf_file)
            jobId = parsed_pdf[0]

            try:
                db.execute(
                    insert_query,
                    parsed_pdf,
                )
                next(counter)
            except duckdb.ConstraintException:
                logger.warning(
                    f"Duplicate entry for jobId: {jobId} Skipping insertion."
                )
            except Exception as e:
                logger.error(f"Error inserting data for jobId: {jobId} - {e}")
    except KeyboardInterrupt:
        logger.info("PDF processing interrupted by user.")
    else:
        logger.success("Data insertion completed.")
    finally:
        logger.info(f"Total records inserted: {counter}")
        db.commit()
        db.close()


if __name__ == "__main__":
    try:
        logger.add(
            REPORTS_DIR / "logs" / "CSC-PDF-processing.log",
            rotation="10 MB",
            retention="10 days",
            level="INFO",
        )
        logger.info("Starting PDF processing.")
        main()
    except Exception as e:
        logger.error(f"Something went wrong: {e}")
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
