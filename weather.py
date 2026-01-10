#!/usr/bin/env python3
"""
Environment Canada Weather Data Collector
Fetches current weather conditions for Metro Vancouver.
"""

import requests
from dataclasses import dataclass
from typing import Optional

# Environment Canada GeoMet API
WEATHER_API_URL = "https://api.weather.gc.ca/collections/citypageweather-realtime/items/bc-74"


@dataclass
class WeatherData:
    """Current weather conditions."""
    temperature_c: Optional[float] = None
    humidity_percent: Optional[int] = None
    wind_speed_kmh: Optional[float] = None
    wind_gust_kmh: Optional[float] = None
    wind_direction: Optional[str] = None
    pressure_kpa: Optional[float] = None
    visibility_km: Optional[float] = None
    condition: Optional[str] = None
    icon_code: Optional[int] = None


def fetch_vancouver_weather() -> WeatherData:
    """Fetch current weather conditions for Vancouver from Environment Canada."""
    response = requests.get(f"{WEATHER_API_URL}?f=json&lang=en", timeout=30)
    response.raise_for_status()
    data = response.json()

    props = data.get("properties", {})
    current = props.get("currentConditions", {})

    def get_value(obj, key="value", lang="en"):
        """Extract value from nested structure."""
        if obj is None:
            return None
        val = obj.get(key, {})
        if isinstance(val, dict):
            return val.get(lang)
        return val

    weather = WeatherData(
        temperature_c=get_value(current.get("temperature")),
        humidity_percent=get_value(current.get("relativeHumidity")),
        wind_speed_kmh=get_value(current.get("wind", {}).get("speed")),
        wind_gust_kmh=get_value(current.get("wind", {}).get("gust")),
        wind_direction=get_value(current.get("wind", {}).get("direction")),
        pressure_kpa=get_value(current.get("pressure")),
        visibility_km=get_value(current.get("visibility")),
        condition=get_value(current.get("condition")),
        icon_code=current.get("iconCode", {}).get("value"),
    )

    return weather


if __name__ == "__main__":
    weather = fetch_vancouver_weather()
    print(f"Vancouver Weather:")
    print(f"  Temperature: {weather.temperature_c}°C")
    print(f"  Humidity: {weather.humidity_percent}%")
    print(f"  Wind: {weather.wind_speed_kmh} km/h {weather.wind_direction}")
    print(f"  Condition: {weather.condition}")
