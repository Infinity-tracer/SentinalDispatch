#!/usr/bin/env python3
"""Weather Data Ingestion Service - Fetches weather data from NOAA API and publishes to Kafka."""
import argparse
import sys
import time
from typing import Dict, Any, Optional, List

import requests

# No need to modify sys.path - we're now inside the package

from sentinel_dispatch.utils.config import load_config
from sentinel_dispatch.utils.kafka_utils import KafkaProducer, ensure_topic_exists
from sentinel_dispatch.utils.logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)


class NOAAWeatherClient:
    """Client for fetching weather data from NOAA API.

    Uses the recommended workflow from NOAA API documentation:
    1. /points/{lat},{lon} - Get grid coordinates and forecast URLs
    2. /gridpoints/{office}/{gridX},{gridY}/forecast/hourly - Get hourly forecast
    3. /alerts/active?point={lat},{lon} - Get active weather alerts
    4. /stations/{stationId}/observations/latest - Get current observations

    See: https://www.weather.gov/documentation/services-web-api#/
    """

    def __init__(self, base_url: str, user_agent: str = "SentinelDispatch/1.0"):
        """
        Initialize NOAA weather client.

        Args:
            base_url: Base URL for NOAA API
            user_agent: User agent string for API requests
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        # Cache grid coordinates to reduce API calls (as recommended by NOAA)
        self._grid_cache: Dict[str, Dict[str, Any]] = {}

    def _get_grid_info(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Get grid information for a location (with caching).

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            Grid information dict with office, gridX, gridY, and forecast URLs
        """
        cache_key = f"{lat:.4f},{lon:.4f}"

        # Check cache first
        if cache_key in self._grid_cache:
            return self._grid_cache[cache_key]

        try:
            # Use /points endpoint to get grid coordinates (as per NOAA documentation)
            points_url = f"{self.base_url}/points/{lat},{lon}"
            response = self.session.get(points_url, timeout=10)
            response.raise_for_status()
            points_data = response.json()

            props = points_data.get("properties", {})
            grid_info = {
                "office": props.get("gridId"),
                "gridX": props.get("gridX"),
                "gridY": props.get("gridY"),
                "forecast": props.get("forecast"),
                "forecastHourly": props.get("forecastHourly"),
                "forecastGridData": props.get("forecastGridData"),
            }

            # Cache the grid info
            self._grid_cache[cache_key] = grid_info
            return grid_info

        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient._get_grid_info.error",
                        lat=lat, lon=lon, error=str(e), exc_info=True)
            return None

    def get_forecast(self, lat: float, lon: float, use_hourly: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get forecast for a location using gridpoints endpoint.

        Uses /gridpoints/{office}/{gridX},{gridY}/forecast/hourly for more granular data
        as recommended by NOAA API documentation.

        Args:
            lat: Latitude
            lon: Longitude
            use_hourly: If True, use hourly forecast (more granular). If False, use 12h periods.

        Returns:
            Forecast data dictionary or None if error
        """
        try:
            grid_info = self._get_grid_info(lat, lon)
            if not grid_info or not grid_info.get("office"):
                return None

            # Use hourly forecast for more granular data (better for disaster scenarios)
            if use_hourly and grid_info.get("forecastHourly"):
                forecast_url = grid_info["forecastHourly"]
            elif grid_info.get("forecast"):
                forecast_url = grid_info["forecast"]
            else:
                logger.warning("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_forecast.no_forecast_url",
                             lat=lat, lon=lon)
                return None

            response = self.session.get(forecast_url, timeout=10)
            response.raise_for_status()
            forecast_data = response.json()

            # Extract current conditions from first period
            periods = forecast_data.get("properties", {}).get("periods", [])
            if periods:
                current = periods[0]
                # Handle both dict and direct value formats for relativeHumidity
                humidity = current.get("relativeHumidity", {})
                if isinstance(humidity, dict):
                    humidity = humidity.get("value", 0.0)

                return {
                    "temperature": current.get("temperature", 0.0),
                    "windSpeed": current.get("windSpeed", "").split()[0] if current.get("windSpeed") else "0",
                    "windDirection": current.get("windDirection", ""),
                    "humidity": float(humidity) if humidity is not None else 0.0,
                    "visibility": current.get("visibility", ""),
                    "shortForecast": current.get("shortForecast", ""),
                    "isDaytime": current.get("isDaytime", True),
                }
            return None

        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_forecast.error",
                        lat=lat, lon=lon, error=str(e), exc_info=True)
            return None

    def get_alerts(self, lat: float, lon: float, date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get weather alerts for a location (active or historical).

        Critical for disaster scenarios - provides fire weather warnings, etc.
        - For current data: Uses /alerts/active?point={lat},{lon}
        - For historical data: Uses /alerts?start={start}&end={end}&point={lat},{lon}

        Note: Historical alerts are only available for the past 7 days via the API.
        For older dates, returns empty list.

        Args:
            lat: Latitude
            lon: Longitude
            date: Optional date in YYYY-MM-DD format. If provided and in the past, retrieves historical alerts.
                  If None or future date, retrieves active alerts.

        Returns:
            List of alert dictionaries
        """
        try:
            from datetime import datetime, timedelta, UTC

            use_historical = False
            if date:
                try:
                    query_date = datetime.strptime(date, "%Y-%m-%d")
                    now = datetime.now(UTC)
                    days_ago = (now.date() - query_date.date()).days

                    # NOAA API provides alerts for past 7 days only
                    if 0 < days_ago <= 7:
                        use_historical = True
                    elif days_ago > 7:
                        logger.warning("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_alerts.date_too_old",
                                     date=date, days_ago=days_ago,
                                     message="Historical alerts only available for past 7 days via API")
                        return []
                except ValueError:
                    logger.warning("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_alerts.invalid_date",
                                 date=date)

            if use_historical:
                # Query historical alerts for the specific date
                start_time = f"{date}T00:00:00Z"
                # End time is start of next day
                query_date = datetime.strptime(date, "%Y-%m-%d")
                end_date = query_date + timedelta(days=1)
                end_time = end_date.strftime("%Y-%m-%dT00:00:00Z")

                alerts_url = f"{self.base_url}/alerts"
                params = {
                    "start": start_time,
                    "end": end_time,
                    "point": f"{lat},{lon}"
                }
                logger.info("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_alerts.fetching_historical",
                           date=date, lat=lat, lon=lon)
            else:
                # Query active alerts
                alerts_url = f"{self.base_url}/alerts/active"
                params = {"point": f"{lat},{lon}"}
                logger.info("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_alerts.fetching_active",
                           lat=lat, lon=lon)

            response = self.session.get(alerts_url, params=params, timeout=10)
            response.raise_for_status()
            alerts_data = response.json()

            alerts = []
            for feature in alerts_data.get("features", []):
                props = feature.get("properties", {})
                alerts.append({
                    "event": props.get("event", ""),
                    "headline": props.get("headline", ""),
                    "description": props.get("description", ""),
                    "severity": props.get("severity", ""),
                    "urgency": props.get("urgency", ""),
                    "certainty": props.get("certainty", ""),
                    "effective": props.get("effective", ""),
                    "expires": props.get("expires", ""),
                    "status": props.get("status", ""),
                })

            if alerts:
                logger.info("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_alerts.found",
                           count=len(alerts), is_historical=use_historical, date=date)

            return alerts

        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_alerts.error",
                        lat=lat, lon=lon, date=date, error=str(e), exc_info=True)
            return []

    def get_observations(self, lat: float, lon: float, date: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get observations for a location (current or historical).

        Uses /points/{lat},{lon} to get observation stations, then:
        - /stations/{stationId}/observations/latest for current conditions
        - /stations/{stationId}/observations?start={date}T00:00:00Z&end={date}T23:59:59Z for historical

        Note: Current observations may be delayed up to 20 minutes from MADIS.
        Historical observations are available for past dates.

        Args:
            lat: Latitude
            lon: Longitude
            date: Optional date in YYYY-MM-DD format. If provided and in the past, retrieves historical data.
                  If None or future date, retrieves latest observation.

        Returns:
            Observation data dictionary or None if error
        """
        try:
            grid_info = self._get_grid_info(lat, lon)
            if not grid_info:
                return None

            # Get stations for location
            points_url = f"{self.base_url}/points/{lat},{lon}"
            response = self.session.get(points_url, timeout=10)
            response.raise_for_status()
            points_data = response.json()

            # Get observations URL
            observations_url = points_data.get("properties", {}).get("observationStations")
            if not observations_url:
                return None

            response = self.session.get(observations_url, timeout=10)
            response.raise_for_status()
            stations_data = response.json()

            if not stations_data.get("features"):
                return None

            station_id = stations_data["features"][0]["properties"]["stationIdentifier"]

            # Determine if we need historical data
            use_historical = False
            if date:
                from datetime import datetime, UTC
                try:
                    query_date = datetime.strptime(date, "%Y-%m-%d")
                    # If date is in the past, use historical endpoint
                    if query_date.date() < datetime.now(UTC).date():
                        use_historical = True
                except ValueError:
                    logger.warning("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_observations.invalid_date",
                                 date=date)

            if use_historical:
                # Get historical observations for the specific date
                # Format: /stations/{stationId}/observations?start={start}&end={end}
                start_time = f"{date}T00:00:00Z"
                end_time = f"{date}T23:59:59Z"
                obs_url = f"{self.base_url}/stations/{station_id}/observations"
                params = {"start": start_time, "end": end_time, "limit": 1}
                logger.info("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_observations.fetching_historical",
                           station_id=station_id, date=date)
            else:
                # Get latest observation
                obs_url = f"{self.base_url}/stations/{station_id}/observations/latest"
                params = None
                logger.info("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_observations.fetching_latest",
                           station_id=station_id)

            response = self.session.get(obs_url, params=params, timeout=10)
            response.raise_for_status()
            obs_data = response.json()

            # Handle both single observation (latest) and collection (historical) responses
            if "properties" in obs_data:
                # Single observation response (latest)
                props = obs_data.get("properties", {})
            elif "features" in obs_data and obs_data["features"]:
                # Collection response (historical) - get the first/closest observation
                props = obs_data["features"][0].get("properties", {})
            else:
                logger.warning("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_observations.no_data",
                             station_id=station_id, date=date, use_historical=use_historical)
                return None

            return {
                "temperature": props.get("temperature", {}).get("value"),
                "windSpeed": props.get("windSpeed", {}).get("value"),
                "windDirection": props.get("windDirection", {}).get("value"),
                "humidity": props.get("relativeHumidity", {}).get("value"),
                "visibility": props.get("visibility", {}).get("value"),
                "stationId": station_id,
                "timestamp": props.get("timestamp", ""),
                "isHistorical": use_historical,
            }

        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_weather.NOAAWeatherClient.get_observations.error",
                        lat=lat, lon=lon, date=date, error=str(e), exc_info=True)
            return None


def format_weather_message(
    location: Dict[str, float],
    weather_data: Dict[str, Any],
    alerts: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Format weather data to match the expected schema.

    Args:
        location: Location dict with lat/lon
        weather_data: Raw weather data from API
        alerts: List of active weather alerts (optional)

    Returns:
        Formatted weather message
    """
    # Convert wind speed to float if it's a string
    wind_speed = weather_data.get("windSpeed")
    if isinstance(wind_speed, str):
        try:
            wind_speed = float(wind_speed.split()[0])
        except (ValueError, IndexError):
            wind_speed = 0.0

    wind_direction = weather_data.get("windDirection")
    humidity = weather_data.get("humidity")
    temperature = weather_data.get("temperature")

    # Use historical timestamp if available, otherwise use current time
    timestamp = weather_data.get("timestamp")
    if timestamp and not timestamp.endswith("Z") and "+" not in timestamp:
        # Ensure timestamp is in ISO format with timezone
        timestamp = timestamp + "Z"

    message = {
        "timestamp": timestamp,
        "location": location,
        "temperature": float(temperature) if temperature is not None else None,
        "windSpeed": float(wind_speed) if wind_speed is not None else None,
        "windDirection": wind_direction,
        "humidity": float(humidity) if humidity is not None else None,
        "visibility": weather_data.get("visibility"),
    }

    # Add alerts if provided (critical for disaster scenarios)
    if alerts:
        message["alerts"] = alerts
        # Flag if there are fire weather warnings
        fire_alerts = [a for a in alerts if "fire" in a.get("event", "").lower() or "red flag" in a.get("headline", "").lower()]
        message["hasFireWeatherWarning"] = len(fire_alerts) > 0

    return message


def fetch_and_publish_weather(
    producer: KafkaProducer,
    client: NOAAWeatherClient,
    location: Dict[str, float],
    topic: str = "weather-data",
    date: Optional[str] = None,
) -> None:
    """
    Fetch weather data and publish to Kafka.

    Args:
        producer: Kafka producer instance
        client: NOAA weather client
        location: Location dict with lat/lon
        topic: Kafka topic name
        date: Optional date in YYYY-MM-DD format. If provided and in the past, retrieves historical data.
    """
    lat = location["lat"]
    lon = location["lon"]
    logger.info("sentinel_dispatch.services.ingest_weather.fetch_and_publish_weather.fetching",
               lat=lat, lon=lon, date=date)

    # Get observations (preferred for current/historical conditions) or forecast as fallback
    # If date is provided, use it for historical observations
    weather_data = client.get_observations(lat, lon, date=date) or client.get_forecast(lat, lon, use_hourly=True)

    if not weather_data:
        logger.warning("sentinel_dispatch.services.ingest_weather.fetch_and_publish_weather.no_data",
                      lat=lat, lon=lon, date=date)
        return

    # Get alerts (active for current data, historical for past dates)
    # Note: Historical alerts only available for past 7 days via NOAA API
    alerts = client.get_alerts(lat, lon, date=date)
    if alerts:
        logger.info("sentinel_dispatch.services.ingest_weather.fetch_and_publish_weather.alerts_found",
                   count=len(alerts), lat=lat, lon=lon, date=date)

    message = format_weather_message(location, weather_data, alerts=alerts)
    location_key = f"{lat:.4f},{lon:.4f}"
    producer.publish(topic, key=location_key, value=message)
    producer.flush()
    logger.info("sentinel_dispatch.services.ingest_weather.fetch_and_publish_weather.published",
               location_key=location_key, lat=lat, lon=lon, has_alerts=len(alerts) > 0)


def main():
    """Main entry point for weather ingestion service."""
    parser = argparse.ArgumentParser(description="Ingest weather data from NOAA API and publish to Kafka")
    parser.add_argument(
        "--topic",
        type=str,
        default="weather-data",
        help="Kafka topic name (default: weather-data)",
    )
    parser.add_argument(
        "--lat",
        type=float,
        help="Latitude for weather data",
    )
    parser.add_argument(
        "--lon",
        type=float,
        help="Longitude for weather data",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=900,
        help="Polling interval in seconds (default: 900 = 15 minutes)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Fetch once and exit (default: continuous polling)",
    )

    args = parser.parse_args()

    try:
        # Load configuration (mode from SENTINEL_MODE env var)
        config = load_config()
        kafka_config = config.kafka
        bootstrap_servers = kafka_config.get("bootstrap_servers", "localhost:9092")

        # Get location from args or config (Maui default)
        if args.lat and args.lon:
            location = {"lat": args.lat, "lon": args.lon}
        else:
            location_config = config.location
            bbox = location_config.get("bounding_box", {})
            # Use center of bounding box
            location = {
                "lat": (bbox.get("north", 20.90) + bbox.get("south", 20.85)) / 2,
                "lon": (bbox.get("east", -156.65) + bbox.get("west", -156.70)) / 2,
            }
            logger.info("sentinel_dispatch.services.ingest_weather.main.using_default_location", location=location)

        # Get event_date from config for historical data
        event_date = config.location.get("event_date")

        # Get NOAA API config
        noaa_config = config.apis.get("noaa", {})
        base_url = noaa_config.get("base_url", "https://api.weather.gov/")
        user_agent = noaa_config.get("user_agent", "SentinelDispatch/1.0")

        # Ensure topic exists
        logger.info("sentinel_dispatch.services.ingest_weather.main.ensuring_topic", topic=args.topic)
        ensure_topic_exists(bootstrap_servers, args.topic)

        # Create producer and client
        producer = KafkaProducer(bootstrap_servers)
        client = NOAAWeatherClient(base_url, user_agent)

        if args.once:
            # Fetch once and exit
            fetch_and_publish_weather(producer, client, location, args.topic, date=event_date)
        else:
            # Continuous polling
            logger.info("sentinel_dispatch.services.ingest_weather.main.starting_continuous", interval=args.interval)
            while True:
                try:
                    fetch_and_publish_weather(producer, client, location, args.topic, date=event_date)
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    logger.info("sentinel_dispatch.services.ingest_weather.main.interrupted")
                    break
                except Exception as e:
                    logger.error("sentinel_dispatch.services.ingest_weather.main.loop_error", error=str(e), exc_info=True)
                    time.sleep(60)  # Wait before retrying

    except KeyboardInterrupt:
        logger.info("sentinel_dispatch.services.ingest_weather.main.interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error("sentinel_dispatch.services.ingest_weather.main.fatal_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

