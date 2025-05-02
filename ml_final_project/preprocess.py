import duckdb
import polars as pl

from ml_final_project.config import (
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    INTERIM_DATA_DIR,
    logger,
)

if __name__ == "__main__":

    try:
        # Load the data as a polars DataFrame
        pdfDbPath = (
            INTERIM_DATA_DIR
            / "CivilServiceCommission"
            / "civilservicecommission_pdfs.duckdb"
        )
        dbPath = (
            RAW_DATA_DIR
            / "CivilServiceCommission"
            / "civilservicecommission.duckdb"
        )

        db = duckdb.connect(dbPath, read_only=True)
        pdfDb = duckdb.connect(pdfDbPath, read_only=True)

        data = (
            db.sql("SELECT * FROM civilservicecommission")
            .pl()
            .unique(subset=["Action"])
            .with_columns([pl.col("Action").cast(pl.Int32).alias("Action")])
        )
        pdfData = pdfDb.sql("SELECT * FROM civilservicecommission_pdfs").pl()

        logger.info(
            f"Loaded {data.shape[0]} rows and {data.shape[1]} columns from the database."
        )
        logger.info(f"Data sample:\n{data.head(2)}")

        joined_data = pdfData.join(
            data, left_on="jobId", right_on="Action", how="inner"
        )

        formatted_data = (
            joined_data.drop(["Agency", "PositionTitle", "PlantillaNo"])
            .with_columns(
                [
                    pl.col("Agency_right").alias("Agency"),
                ]
            )
            .drop(["Agency_right"])
            .with_columns(
                [
                    pl.col("Posting Date")
                    .str.strptime(
                        pl.Date, "%d %b %Y"
                    )  # Convert from "20 Apr 2025"
                    .alias("Posting Date"),
                    pl.col("Closing Date")
                    .str.strptime(
                        pl.Date, "%d %b %Y"
                    )  # Convert from "20 Apr 2025"
                    .alias("Closing Date"),
                ]
            )
            .select(
                [
                    pl.col("jobId"),
                    pl.col("Agency"),
                    pl.col("Region"),
                    pl.col("PlaceOfAssignment"),
                    pl.col("Posting Date"),
                    pl.col("Closing Date"),
                    pl.col("Position Title"),
                    pl.col("SalaryGrade"),
                    pl.col("MonthlySalary"),
                    pl.col("Eligibility"),
                    pl.col("Education"),
                    pl.col("Training"),
                    pl.col("Experience"),
                    pl.col("Competency"),
                    pl.col("Plantilla Item No."),
                ]
            )
        ).unique(subset=["jobId"])

        logger.info(f"Converted data sample:\n{formatted_data.head(2)}")
        logger.info(
            f"Loaded {formatted_data.shape[0]} rows and {formatted_data.shape[1]} columns from the database."
        )

        # Save the formatted data to a CSV file
        SAVE_PATH = PROCESSED_DATA_DIR / "CivilServiceCommission"
        if not SAVE_PATH.exists():
            SAVE_PATH.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory for the CSV file: {SAVE_PATH}")

        formatted_data.write_parquet(
            SAVE_PATH / "civilservicecommission.parquet"
        )

        logger.success(
            f"Saved the formatted data to {SAVE_PATH / "civilservicecommission.parquet"}."
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        db.close()
        logger.info("Closed the database connection.")
