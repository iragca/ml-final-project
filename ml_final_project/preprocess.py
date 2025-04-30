import duckdb
import polars as pl

from ml_final_project.config import RAW_DATA_DIR, PROCESSED_DATA_DIR, logger

if __name__ == "__main__":

    try:
        # Load the data as a polars DataFrame
        dbPath = (
            RAW_DATA_DIR
            / "CivilServiceCommission"
            / "civilservicecommission.duckdb"
        )
        db = duckdb.connect(dbPath, read_only=True)
        data = db.sql("SELECT * FROM civilservicecommission").pl()

        logger.info(
            f"Loaded {data.shape[0]} rows and {data.shape[1]} columns from the database."
        )
        logger.info(f"Data sample:\n{data.head(2)}")

        # Convert the "Posting Date" and "Closing Date" columns to datetime
        formatted_data = data.with_columns(
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
                pl.col("Action").cast(pl.Int32).alias("Action"),
            ]
        ).unique(subset=["Action"])

        logger.info(f"Converted data sample:\n{formatted_data.head(2)}")
        logger.info(
            f"Loaded {formatted_data.shape[0]} rows and {formatted_data.shape[1]} columns from the database."
        )

        # Save the formatted data to a CSV file
        formatted_data.write_csv(
            PROCESSED_DATA_DIR / "civilservicecommission.csv"
        )

        logger.success(
            f"Saved the formatted data to {PROCESSED_DATA_DIR / 'civilservicecommission.csv'}."
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        db.close()
        logger.info("Closed the database connection.")
