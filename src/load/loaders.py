# src/load/loaders.py
import logging
from pathlib import Path

import pandas as pd
from config.config import Config

logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self):
        self.output_path = Path(Config.OUTPUT_PATH)
        self.output_path.mkdir(parents=True, exist_ok=True)

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        try:
            file_path = self.output_path / f"{filename}.csv"
            df.to_csv(file_path, index=False)
            logger.info(f"Data saved successfully to {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"Error saving CSV file {filename}: {str(e)}")
            raise
