import time
import requests
from typing import Dict


class OpenWeatherClient:
    """
    HTTP client for the OpenWeather Current Weather API.

    This client implements:
        - request retries
        - fixed backoff between attempts
        - timeout handling
        - API error propagation

    The goal is to make the ingestion pipeline resilient to:
        - transient network failures
        - rate limiting
        - short API outages
        - DNS issues

    The client is intentionally stateless so it can be reused across
    multiple cities inside a single pipeline execution.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        units: str,
        timeout: int,
        max_retries: int,
        backoff_seconds: int,
        logger
    ):
        """
        Initialize the API client configuration.

        Args:
            base_url: OpenWeather endpoint URL.
            api_key: OpenWeather API key.
            units: Measurement units (metric, imperial, default).
            timeout: HTTP timeout in seconds.
            max_retries: Maximum retry attempts per request.
            backoff_seconds: Sleep duration between retries.
            logger: Preconfigured application logger.

        Notes:
            The client does NOT validate the API key here — validation
            happens naturally during the first request to avoid startup latency.
        """
        self.base_url = base_url
        self.api_key = api_key
        self.units = units
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.logger = logger

    def fetch_weather(self, city: str, country: str) -> Dict:
        """
        Fetch current weather data for a given city.

        A retry loop is used because weather ingestion pipelines usually run
        on schedules. A single temporary network glitch should
        not fail the whole execution.

        Args:
            city: City name (e.g., "Lisbon").
            country: ISO 3166 country code (e.g., "PT").

        Returns:
            Parsed JSON response from OpenWeather API.

        Raises:
            RuntimeError: If all retry attempts fail.

        Retry Strategy:
            Fixed backoff (simple + predictable)
            Attempt count controlled by config
            Fail fast after final attempt
        """
        # OpenWeather expects "City,CountryCode"
        query = f"{city},{country}"

        params = {
            "q": query,
            "appid": self.api_key,
            "units": self.units
        }

        # Retry loop — protects pipeline reliability
        for attempt in range(1, self.max_retries + 1):
            try:
                # External network call (primary failure point)
                response = requests.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout
                )

                # Raises HTTPError for 4xx/5xx
                response.raise_for_status()

                # Success path
                return response.json()

            except requests.RequestException as e:
                # Includes:
                # - Timeout
                # - DNS failure
                # - Connection error
                # - HTTP error status
                self.logger.warning(
                    f"Attempt {attempt}/{self.max_retries} failed for {query}: {e}"
                )

                # Do not sleep after final attempt
                if attempt < self.max_retries:
                    time.sleep(self.backoff_seconds)

        # After exhausting retries we fail the city, not silently ignore it
        raise RuntimeError(f"Failed to fetch data for {query}")

    def get_field(self, d, *keys, default=None):
        """
        Safely extract nested dictionary values from API responses.

        Avoids repetitive defensive checks when parsing JSON payloads.

        Example:
            get_field(raw, "main", "temp")
            get_field(raw, "weather", 0, "description")

        Args:
            d: Source dictionary.
            *keys: Path of nested keys/indexes.
            default: Value returned if any level is missing.

        Returns:
            Extracted value or default.
        """
        for key in keys:
            if isinstance(d, dict) and key in d:
                d = d[key]
            elif isinstance(d, list) and isinstance(key, int) and 0 <= key < len(d):
                d = d[key]
            else:
                return default
        return d