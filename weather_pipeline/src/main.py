import os
from datetime import datetime, timezone

from src.api_client import OpenWeatherClient # Handles API communication
from src.config_loader import ConfigLoader # Handles validation and city list loading
from src.logger import setup_logger # Handles logging to stdout stream
from src.models import WeatherRecord # Handles formatting of weather data records
from src.storage import Storage # Handles output files saving

# -------------------------------
# Setup config loader and logger
# -------------------------------
config_loader = ConfigLoader(
    config_path    = "config/config.yaml",
    city_list_path = "config/city.list.json"
)

# Load configuration and valid city set
config = config_loader.load_config()
config_loader.load_valid_cities()

logger = setup_logger()

# -------------------------------
# Validate cities from YAML
# -------------------------------
print("\nValidating cities in YAML config...\n")

invalid_cities = [
    f"{c['country'].upper()}.{c['name']}"
    for c in config["cities"]
    if not config_loader.validate_city(c["name"], c["country"])
]

if invalid_cities:
    print("⚠️ The following cities are invalid (not in OpenWeather list):")

    for c in invalid_cities:
        print(f" - {c}")
    print("\nThese cities will be skipped.\n")

else:
    print("✅ All cities in config.yaml are valid according to OpenWeather city list.\n")

# Only process valid cities
valid_cities_list = [
    c for c in config["cities"]
    if config_loader.validate_city(c["name"], c["country"])
]

# -------------------------------
# Initialize OpenWeather API client
# -------------------------------
api_key = os.environ.get("OPENWEATHER_API_KEY")
if not api_key:
    raise RuntimeError("OPENWEATHER_API_KEY environment variable is not set")

client = OpenWeatherClient(
    base_url        = config["api"]["base_url"],
    api_key         = api_key,
    units           = config["api"]["units"],
    timeout         = config["api"]["timeout_seconds"],
    max_retries     = config["api"]["max_retries"],
    backoff_seconds = config["api"]["backoff_seconds"],
    logger          = logger
)

# -------------------------------
# Ingest weather data
# -------------------------------
success_count = 0
failed_cities_count = 0
failed_cities = []
records = []

print("Starting weather ingestion pipeline...\n")

for city_info in valid_cities_list:
    city_name   = city_info["name"]
    country_code = city_info["country"].upper()

    print(f"Fetching weather for {city_name}, {country_code}...")

    try:
        raw = client.fetch_weather(city_name, country_code)

        # Default values for missing fields
        default_none = None

        record = WeatherRecord(
            city                   = city_name,
            country                = country_code,
            timestamp              = datetime.fromtimestamp(client.get_field(raw, "dt", default = default_none), tz = timezone.utc),
            timezone_offset        = client.get_field(raw, "timezone", default = default_none),
            weather                = client.get_field(raw, "weather", 0, "main", default = default_none),
            weather_description    = client.get_field(raw, "weather", 0, "description", default = default_none),
            temperature_min        = client.get_field(raw, "main", "temp_min", default = default_none),
            temperature_max        = client.get_field(raw, "main", "temp_max", default = default_none),
            temperature_current    = client.get_field(raw, "main", "temp", default = default_none),
            temperature_feels_like = client.get_field(raw, "main", "feels_like", default = default_none),
            cloudiness             = client.get_field(raw, "clouds", "all", default = default_none),
            wind_speed             = client.get_field(raw, "wind", "speed", default = default_none),
            wind_direction_deg     = client.get_field(raw, "wind", "deg", default = default_none),
            humidity               = client.get_field(raw, "main", "humidity", default = default_none),
            pressure               = client.get_field(raw, "main", "pressure", default = default_none),
            sunrise                = datetime.fromtimestamp(client.get_field(raw, "sys", "sunrise", default = default_none), tz = timezone.utc),
            sunset                 = datetime.fromtimestamp(client.get_field(raw, "sys", "sunset", default = default_none), tz = timezone.utc),
            units                  = client.units
        )

        records.append(record)
        success_count += 1

    except Exception as e:
        logger.error(f"Failed processing {city_name}, {country_code}: {e}")
        failed_cities.append(f"{country_code}.{city_name}")
        failed_cities_count += 1

# -------------------------------
# Write to storage
# -------------------------------
storage = Storage(
    base_path = config["storage"]["base_path"],
    fmt       = config["storage"]["format"],
    layout    = config["storage"]["layout"]
)

storage.write(records)

# -------------------------------
# Summary
# -------------------------------
print("\nPipeline finished.\n")
print(f"✅ Successful cities: {success_count}")
print(f"❌ Failed cities: {failed_cities_count}\n")

if failed_cities_count > 0:
    print("List of failed cities (check names/country codes):")

    for c in failed_cities:
        print(f" - {c}")

logger.info("Weather ingestion completed")
