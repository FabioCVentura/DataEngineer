from datetime import datetime
from pydantic import BaseModel
from typing import Optional

class WeatherRecord(BaseModel):
    """
    Validated weather record with automatic formatting for timestamps,
    timezone, wind direction, and units.
    """
    city: Optional[str]
    country: Optional[str]
    timestamp: Optional[datetime]
    timezone_offset: Optional[int]
    weather: Optional[str]
    weather_description: Optional[str]
    temperature_min: Optional[float]
    temperature_max: Optional[float]
    temperature_current: Optional[float]
    temperature_feels_like: Optional[float]
    cloudiness: Optional[int]
    wind_speed: Optional[float]
    wind_direction_deg: Optional[int]
    humidity: Optional[int]
    pressure: Optional[int]
    sunrise: Optional[datetime]
    sunset: Optional[datetime]
    units: Optional[str]

    def format_datetime(self, dt: datetime) -> str:
        """Return datetime in 'dd-MM-yyyy HH:mm:ss' format."""
        return dt.strftime("%d-%m-%Y %H:%M:%S")

    def timezone_str(self) -> str:
        """Convert seconds offset to UTC string."""
        hours = self.timezone_offset // 3600
        if hours == 0:
            return "UTC"
        sign = "+" if hours > 0 else "-"
        return f"UTC{sign}{abs(hours)}"

    def wind_direction_compass(self) -> str:
        """
        Convert wind degrees to full compass direction name (16 points).
        0° / 360° -> North
        22.5°    -> North-Northeast
        45°      -> Northeast
        67.5°    -> East-Northeast
        90°      -> East
        ...
        337.5°  -> North-Northwest
        """

        dirs = [
            "North", "North-Northeast", "Northeast", "East-Northeast",
            "East", "East-Southeast", "Southeast", "South-Southeast",
            "South", "South-Southwest", "Southwest", "West-Southwest",
            "West", "West-Northwest", "Northwest", "North-Northwest"
        ]
        # Each sector is 22.5°, divide wind degrees by 22.5 and round to nearest integer
        ix = round(self.wind_direction_deg / 22.5) % 16
        return dirs[ix]

    def field_with_units(self, field_name: str) -> str: #TODO - check units
        """
        Return a human-readable field value with units, dynamically
        using the unit system specified in self.units (default, metric, imperial).
        """
        # Determine temperature unit based on the API units
        if "temperature" in field_name:
            temp_unit = {
                "metric": "°C",
                "imperial": "°F",
                "default": "K"
            }.get(self.units, "K")
            unit = temp_unit
        elif field_name == "wind_speed":
            unit = "m/s" if self.units in ("metric", "default") else "mph"
        elif field_name in ["humidity", "cloudiness"]:
            unit = "%"
        elif field_name == "pressure":
            unit = "hPa"
        else:
            unit = ""

        value = getattr(self, field_name)
        return f"{value} {unit}" if unit else str(value)

    def formatted_record(self) -> dict:
        """
        Return a dictionary suitable for CSV or display.
        Units are added to the **header names**, values remain numeric.
        """
        return {
            "City": self.city,
            "Country": self.country,
            "Timestamp": self.format_datetime(self.timestamp),
            "Timezone": self.timezone_str(),
            "Weather": self.weather,
            "Weather_Description": self.weather_description,
            "Cloudiness_(%)": self.cloudiness,
            f"Temperature_Current_({self.field_with_units('temperature_current').split()[-1]})": self.temperature_current,
            f"Temperature_Min_({self.field_with_units('temperature_min').split()[-1]})": self.temperature_min,
            f"Temperature_Max_({self.field_with_units('temperature_max').split()[-1]})": self.temperature_max,
            f"Temperature_Feels_Like_({self.field_with_units('temperature_feels_like').split()[-1]})": self.temperature_feels_like,
            f"Wind_Speed_({self.field_with_units('wind_speed').split()[-1]})": self.wind_speed,
            "Wind_Direction": self.wind_direction_compass(),
            "Humidity_(%)": self.humidity,
            "Pressure_(hPa)": self.pressure,
            "Sunrise": self.format_datetime(self.sunrise),
            "Sunset": self.format_datetime(self.sunset),
        }

