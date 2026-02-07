import pandas as pd
from pathlib import Path
from typing import List
from src.models import WeatherRecord
from datetime import datetime, timezone


class Storage:
    """
    Storage abstraction responsible for persisting weather data.

    This class supports multiple file formats (CSV, JSON, Parquet) and
    multiple folder layout options for Hive-style partitioning.
    """

    def __init__(self, base_path: str, fmt: str, layout: str):
        """
        Initialize storage.

        Args:
            base_path (str): Base path where data will be saved
            fmt (str): File format to write ('csv', 'json', 'parquet')
            layout (str): Folder layout option. Supported:
                - date: year/month/day/weather_timestamp
                - date_country: year/month/day/country_weather_timestamp
                - country_date: country/year/month/day/weather_timestamp
                - hive_compact: year/month/day/country/city_weather_timestamp
                - city_date: city/year/month/day/weather_timestamp
        """
        self.base_path = Path(base_path)
        self.fmt = fmt.lower()
        self.layout = layout.lower()

    def _save(self, df: pd.DataFrame, filepath: Path) -> None:
        """Save a DataFrame to disk in the configured format."""
        if self.fmt == "parquet":
            df.to_parquet(filepath, index=False)
        elif self.fmt == "csv":
            df.to_csv(filepath, index=False)
        elif self.fmt == "json":
            df.to_json(filepath, orient="records", lines=True)
        else:
            raise ValueError(f"Unsupported format: {self.fmt}")

    def write(self, records: List[WeatherRecord]) -> None:
        """
        Write weather records to disk using the configured format and layout.

        Args:
            records (List[WeatherRecord]): List of WeatherRecord objects
        """
        if not records:
            return

        # Convert records to dicts
        data = [r.formatted_record() for r in records]
        df = pd.DataFrame(data)

        # Preserve display column
        df["_ts"] = pd.to_datetime(df["Timestamp"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
        df = df.dropna(subset=["_ts"])

        run_ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        # Grouping logic based on layout
        if self.layout == "date":
            for date, group in df.groupby(df["_ts"].dt.date):
                path = self.base_path / f"year={date.year}/month={date.month}/day={date.day}"
                path.mkdir(parents=True, exist_ok=True)
                filename = f"weather_{run_ts}.{self.fmt}"
                self._save(group.drop(columns=["_ts"]), path / filename)

        elif self.layout == "date_country":
            for (date, country), group in df.groupby([df["_ts"].dt.date, "Country"]):
                path = self.base_path / f"year={date.year}/month={date.month}/day={date.day}"
                path.mkdir(parents=True, exist_ok=True)
                filename = f"{country}_weather_{run_ts}.{self.fmt}"
                self._save(group.drop(columns=["_ts"]), path / filename)

        elif self.layout == "country_date":
            for (country, date), group in df.groupby([df["Country"], df["_ts"].dt.date]):
                path = self.base_path / f"{country}/year={date.year}/month={date.month}/day={date.day}"
                path.mkdir(parents=True, exist_ok=True)
                filename = f"weather_{run_ts}.{self.fmt}"
                self._save(group.drop(columns=["_ts"]), path / filename)

        elif self.layout == "hive_compact":
            for _, row in df.iterrows():
                ts = row["_ts"]
                city = row["City"]
                country = row["Country"]
                path = self.base_path / f"year={ts.year}/month={ts.month}/day={ts.day}/{country}/{city}"
                path.mkdir(parents=True, exist_ok=True)
                filename = f"{country}_{city}_{run_ts}.{self.fmt}"
                self._save(pd.DataFrame([row]).drop(columns=["_ts"]), path / filename)

        elif self.layout == "city_date":
            for (city, date), group in df.groupby([df["City"], df["_ts"].dt.date]):
                path = self.base_path / f"{city}/year={date.year}/month={date.month}/day={date.day}"
                path.mkdir(parents=True, exist_ok=True)
                filename = f"weather_{run_ts}.{self.fmt}"
                self._save(group.drop(columns=["_ts"]), path / filename)

        else:
            raise ValueError(f"Unsupported layout: {self.layout}")
