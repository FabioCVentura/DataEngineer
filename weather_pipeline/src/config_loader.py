import json
import yaml
import unicodedata
from pathlib import Path
from typing import Dict, Set, Tuple


class ConfigLoader:
    """
    Handles loading and validation of configuration and city list data.

    Responsibilities:
        - Load YAML configuration.
        - Load OpenWeather `city.list.json` and build a normalized set for validation.
        - Provide a method to validate if a city exists in OpenWeather dataset.
    """

    def __init__(self, config_path: str, city_list_path: str):
        """
        Initialize the ConfigLoader.

        Args:
            config_path (str): Path to the YAML configuration file.
            city_list_path (str): Path to the OpenWeather city list JSON file.
        """
        self.config_path      = Path(config_path)
        self.city_list_path   = Path(city_list_path)
        self.config           = None
        self.valid_cities_set = None

    def load_config(self) -> Dict:
        """
        Load YAML configuration file.

        Returns:
            dict: Parsed configuration dictionary.

        Raises:
            FileNotFoundError: If the YAML file does not exist.
            yaml.YAMLError: If the YAML is invalid.
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        return self.config

    @staticmethod
    # The following function only normalizes data and it is not dependent on any instance attributes like config_path or valid_cities_set, that is why it is a static method
    def normalize_city(name: str) -> str:
        """
        Normalize city names for consistent comparison.

        Converts to ASCII, strips accents, trims whitespace, and lowercases.
        Example:
            "Köln" -> "koln"
            "  Lisboa " -> "lisboa"

        Args:
            name (str): Raw city name.

        Returns:
            str: Normalized city name.
        """
        return (
            unicodedata.normalize("NFKD", name)
            .encode("ASCII", "ignore")
            .decode("utf-8")
            .strip()
            .lower()
        )

    def load_valid_cities(self) -> Set[Tuple[str, str]]:
        """
        Load OpenWeather city list and build a set for validation.

        Each entry in the set is a tuple:
            (normalized_city_name, ISO_country_code)

        Returns:
            set[tuple[str, str]]: Set of valid city-country tuples.

        Raises:
            FileNotFoundError: If the JSON city list does not exist.
            json.JSONDecodeError: If the JSON is invalid.
        """
        if not self.city_list_path.exists():
            raise FileNotFoundError(f"City list not found: {self.city_list_path}")

        with open(self.city_list_path, "r", encoding="utf-8") as f:
            city_list = json.load(f)

        self.valid_cities_set = {
            (self.normalize_city(c["name"]), c["country"].upper())
            for c in city_list
        }

        return self.valid_cities_set

    def validate_city(self, city: str, country: str) -> bool:
        """
        Validate if a city exists in the OpenWeather reference dataset.

        Args:
            city (str): City name from configuration.
            country (str): ISO country code.
        Returns:
            bool: True if city exists in OpenWeather dataset, False otherwise.

        Raises:
            RuntimeError: If `valid_cities_set` is not loaded before calling.
        """
        if self.valid_cities_set is None:
            raise RuntimeError("Valid cities set not loaded. Call load_valid_cities() first.")

        return (self.normalize_city(city), country.upper()) in self.valid_cities_set
