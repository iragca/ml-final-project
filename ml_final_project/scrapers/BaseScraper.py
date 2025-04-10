import re
import time
from io import StringIO

import duckdb
import pandas as pd
import requests
from loguru import logger

from ml_final_project.config import RAW_DATA_DIR


class BaseScraper:

    def __init__(self, name: str, url: str):
        self.NAME = name
        self.URL = url
        self.DATA_DIR = RAW_DATA_DIR / self.NAME
        self.PUBLIC_IP = requests.get("https://api.ipify.org").text

        if " " in self.NAME:
            logger.error("Name cannot contain spaces.")
            raise ValueError("Name cannot contain spaces.")

        logger.info(f"STARTING SCRAPING {self.NAME}")
        logger.info(f"SCRAPING FROM {self.PUBLIC_IP}")
        logger.info(f"SCRAPING URL {self.URL}")

        if not self.DATA_DIR.exists():
            logger.info(f"Creating directory {self.DATA_DIR}")
            self.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def __str__(self):
        return f"<{self.NAME}Scraper({self.url=})>"

    def __repr__(self):
        return f"<{self.NAME}Scraper({self.url=})>"

    def save_to_csv(self, page):
        """Save the DataFrame to a CSV file.
        Args:
            df (pd.DataFrame): DataFrame to save.
            filename (str): Filename for the CSV file.
        """
        data = pd.read_html(StringIO(page.content()))[0]
        logger.info(f"Retrieved data's shape {data.shape}")

        buttons = page.locator("button").all()

        # Regex to match buttons with 'id_' followed by dynamic jobId
        pattern = re.compile(
            r"^info_\d+$"
        )  # You can adjust the pattern to suit your jobId format

        # Iterate over all buttons and check their 'id' attribute using regex
        infoIds = []
        for button in buttons:
            button_id = button.get_attribute("id")
            if button_id and pattern.match(button_id):
                infoIds.append(button_id[5:])

        data["Details"] = infoIds

        # Convert all columns to string type
        # This is to avoid issues with CSV writing
        # due to the nature of dirty data
        data = data.astype(str)

        filecount = sum(1 for _ in self.DATA_DIR.iterdir())
        FILENAME = self.DATA_DIR / f"{self.NAME}.csv"

        if filecount == 0:
            data.to_csv(FILENAME, index=False)
        else:
            csv = pd.read_csv(FILENAME)
            csv = pd.concat([csv, data], ignore_index=True)
            csv.to_csv(FILENAME, index=False)

    def save_to_duckdb(self, page):
        """Save the DataFrame to a DuckDB file.
        Args:
            df (pd.DataFrame): DataFrame to save.
            filename (str): Filename for the DuckDB file.
        """
        data = pd.read_html(StringIO(page.content()))[0]
        logger.info(f"Retrieved data's shape {data.shape}")

        buttons = page.locator("button").all()

        # Regex to match buttons with 'id_' followed by dynamic jobId
        pattern = re.compile(
            r"^info_\d+$"
        )  # You can adjust the pattern to suit your jobId format

        # Iterate over all buttons and check their 'id' attribute using regex
        infoIds = []
        for button in buttons:
            button_id = button.get_attribute("id")
            if button_id and pattern.match(button_id):
                infoIds.append(button_id[5:])

        data["Action"] = infoIds
        print(data)

        # Convert all columns to string type
        # This is to avoid issues with CSV writing
        # due to the nature of dirty data
        data = data.astype(str)

        filecount = sum(1 for _ in self.DATA_DIR.iterdir())
        FILENAME = self.DATA_DIR / f"{self.NAME}.duckdb"

        duckdb_conn = duckdb.connect(FILENAME)
        if filecount == 0:
            duckdb_conn.execute(
                f"CREATE TABLE {self.NAME} AS SELECT * FROM data"
            )
        else:
            duckdb_conn.execute(f"INSERT INTO {self.NAME} SELECT * FROM data")

        duckdb_conn.close()
