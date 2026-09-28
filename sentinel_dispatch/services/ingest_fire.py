#!/usr/bin/env python3
"""Fire Data Ingestion Service - Fetches fire data from FIRMS API and publishes to Kafka."""
import argparse
import csv
import sys
import time
from datetime import UTC, datetime
from typing import Dict, Any, List, Optional

import requests

# No need to modify sys.path - we're now inside the package

from sentinel_dispatch.utils.config import load_config
from sentinel_dispatch.utils.kafka_utils import KafkaProducer, ensure_topic_exists
from sentinel_dispatch.utils.logging_config import configure_logging, get_logger

# Configure logging
configure_logging()
logger = get_logger(__name__)


class FIRMSClient:
    """Client for fetching fire data from FIRMS API.

    Uses the FIRMS area API which requires a MAP_KEY.
    See: https://firms.modaps.eosdis.nasa.gov/api/area/
    """

    def __init__(self, base_url: str, map_key: Optional[str] = None):
        """
        Initialize FIRMS client.

        Args:
            base_url: Base URL for FIRMS API
            map_key: MAP_KEY (required for area API, get free key from FIRMS)
        """
        self.base_url = base_url.rstrip("/")
        self.map_key = map_key
        if not self.map_key:
            logger.warning("sentinel_dispatch.services.ingest_fire.FIRMSClient.__init__.no_map_key", 
                          message="MAP_KEY is required for FIRMS area API. Get a free key from https://firms.modaps.eosdis.nasa.gov/api/")
        self.session = requests.Session()

    def get_fire_data(
        self,
        source: str = "VIIRS_SNPP_NRT",
        days: int = 1,
        bbox: Optional[Dict[str, float]] = None,
        date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get fire detection data from FIRMS area API.

        Args:
            source: Data source. Common options:
                   - VIIRS_SNPP_NRT (Near Real-Time, Suomi-NPP satellite)
                   - VIIRS_NOAA20_NRT (Near Real-Time, NOAA-20 satellite)
                   - MODIS_NRT (Near Real-Time, Terra/Aqua satellites)
                   - VIIRS_SNPP_SP (Standard Processing, Suomi-NPP)
                   - VIIRS_NOAA20_SP (Standard Processing, NOAA-20)
            days: Number of days to query (1-10)
            bbox: Bounding box dict with north, south, east, west. If None, uses 'world'
            date: Optional date in YYYY-MM-DD format. If None, returns most recent data

        Returns:
            List of fire detection records

        See: https://firms.modaps.eosdis.nasa.gov/api/area/
        """
        if not self.map_key:
            logger.error("sentinel_dispatch.services.ingest_fire.FIRMSClient.get_fire_data.no_map_key",
                        message="MAP_KEY is required. Get a free key from https://firms.modaps.eosdis.nasa.gov/api/")
            return []

        try:
            # Determine area coordinates
            if bbox:
                area_coords = f"{bbox['west']},{bbox['south']},{bbox['east']},{bbox['north']}"
            else:
                # Use world coordinates
                area_coords = "-180,-90,180,90"

            # Build URL - FIRMS area API format: /api/area/csv/{MAP_KEY}/{SOURCE}/{AREA_COORDINATES}/{DAY_RANGE}
            if date:
                # With specific date: /api/area/csv/{MAP_KEY}/{SOURCE}/{AREA_COORDINATES}/{DAY_RANGE}/{DATE}
                url = f"{self.base_url}/area/csv/{self.map_key}/{source}/{area_coords}/{days}/{date}"
            else:
                # Most recent data
                url = f"{self.base_url}/area/csv/{self.map_key}/{source}/{area_coords}/{days}"

            logger.info("sentinel_dispatch.services.ingest_fire.FIRMSClient.get_fire_data.fetching", 
                       url=url, source=source, days=days, bbox=bbox)
            response = self.session.get(url, timeout=60)  # Increased timeout for large requests
            response.raise_for_status()

            # Parse CSV response
            lines = response.text.strip().split("\n")
            if len(lines) < 2:
                logger.warning("sentinel_dispatch.services.ingest_fire.FIRMSClient.get_fire_data.no_data")
                return []

            # Parse header
            headers = [h.strip().strip('"') for h in lines[0].split(",")]
            fires = []

            # Parse data rows (handle quoted CSV values)
            csv_reader = csv.reader(lines[1:], quotechar='"')
            for row in csv_reader:
                if not row or len(row) != len(headers):
                    continue
                fire_dict = dict(zip(headers, [v.strip() for v in row]))
                fires.append(fire_dict)

            logger.info("sentinel_dispatch.services.ingest_fire.FIRMSClient.get_fire_data.retrieved", count=len(fires))
            return fires

        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_fire.FIRMSClient.get_fire_data.error", error=str(e), exc_info=True)
            return []

def format_fire_message(fire_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format fire data to match the expected schema.

    Args:
        fire_data: Raw fire data from FIRMS API

    Returns:
        Formatted fire message
    """
    # Extract coordinates (FIRMS API uses 'latitude' and 'longitude')
    try:
        lat = float(fire_data.get("latitude") or fire_data.get("lat") or 0)
        lon = float(fire_data.get("longitude") or fire_data.get("lon") or 0)
    except (ValueError, TypeError):
        logger.warning("sentinel_dispatch.services.ingest_fire.format_fire_message.invalid_coordinates", fire_data=fire_data)
        lat, lon = 0.0, 0.0

    # Generate unique fire ID from coordinates and timestamp
    acq_date = fire_data.get("acq_date", "")
    acq_time = fire_data.get("acq_time", "")
    fire_id = fire_data.get("fireId") or fire_data.get("id") or f"fire-{acq_date}-{acq_time}-{lat:.4f}-{lon:.4f}"

    # Extract confidence (FIRMS uses 'confidence' field, typically 0-100)
    confidence = 0.0
    if fire_data.get("confidence"):
        try:
            confidence = float(fire_data.get("confidence"))
        except (ValueError, TypeError):
            pass

    # Extract brightness temperature (FIRMS uses 'brightness' or 'bright_t31' for MODIS)
    brightness = 0.0
    if fire_data.get("brightness"):
        try:
            brightness = float(fire_data.get("brightness"))
        except (ValueError, TypeError):
            pass
    elif fire_data.get("bright_t31"):  # MODIS uses bright_t31
        try:
            brightness = float(fire_data.get("bright_t31"))
        except (ValueError, TypeError):
            pass

    # Parse timestamp (FIRMS format: acq_date + acq_time)
    timestamp_str = fire_data.get("acq_date") or fire_data.get("timestamp") or ""
    acq_time_str = fire_data.get("acq_time", "00:00")

    if timestamp_str:
        # FIRMS format: YYYY-MM-DD and HHMM (24-hour format)
        if "T" not in timestamp_str and acq_time_str:
            # Combine date and time: YYYY-MM-DDTHH:MM:SSZ
            try:
                # acq_time is in HHMM format (e.g., "1430" = 14:30)
                if len(acq_time_str) == 4:
                    time_formatted = f"{acq_time_str[:2]}:{acq_time_str[2:]}:00"
                else:
                    time_formatted = f"{acq_time_str}:00" if ":" not in acq_time_str else acq_time_str
                timestamp_str = f"{timestamp_str}T{time_formatted}Z"
            except Exception:
                timestamp_str = f"{timestamp_str}T00:00:00Z"
        elif not timestamp_str.endswith("Z"):
            timestamp_str = timestamp_str + "Z"
    else:
        timestamp_str = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    # Determine fire type from source
    source = fire_data.get("satellite", "").upper()
    fire_type = "unknown"
    if "VIIRS" in source:
        fire_type = "viirs"
    elif "MODIS" in source:
        fire_type = "modis"

    # Calculate estimated spread rate from FRP (Fire Radiative Power)
    # FRP is in MW (megawatts) - higher FRP indicates more intense fires
    # Rough estimation: FRP can be used as a proxy for fire intensity/spread potential
    frp = float(fire_data.get("frp", 0.0)) if fire_data.get("frp") else 0.0
    # Estimate spread rate from FRP (rough approximation: higher FRP = higher spread potential)
    # This is a heuristic - actual spread rate depends on many factors (wind, fuel, terrain)
    estimated_spread_rate = 0.0
    if frp > 0:
        # Rough conversion: FRP (MW) to estimated spread rate (km/h)
        # Very rough heuristic: 1 MW ≈ 0.1-0.2 km/h potential spread rate
        # Higher FRP fires are more intense and can spread faster under favorable conditions
        estimated_spread_rate = min(20.0, frp * 0.15)  # Cap at 20 km/h
    
    return {
        "timestamp": timestamp_str,
        "fireId": fire_id,
        "location": {"lat": lat, "lon": lon},
        "confidence": confidence,
        "brightness": brightness,
        "fireType": fire_type,
        "perimeter": [],  # FIRMS doesn't provide perimeter, would need additional processing
        "spreadRate": estimated_spread_rate,  # Estimated from FRP
        "estimatedSize": 0.0,  # FIRMS doesn't provide size directly
        # Include FRP (Fire Radiative Power) from FIRMS API - primary indicator of fire intensity
        "frp": frp,  # Fire Radiative Power in MW
        "satellite": fire_data.get("satellite", ""),
    }


def fetch_and_publish_fires(
    producer: KafkaProducer,
    client: FIRMSClient,
    bbox: Optional[Dict[str, float]] = None,
    topic: str = "fire-data",
    source: str = "VIIRS_SNPP_NRT",
    days: int = 1,
    date: Optional[str] = None,
) -> None:
    """
    Fetch fire data and publish to Kafka.

    Args:
        producer: Kafka producer instance
        client: FIRMS client
        bbox: Bounding box for filtering fires (west, south, east, north). If None, uses world
        topic: Kafka topic name
        source: Data source (default: VIIRS_SNPP_NRT for near real-time)
        days: Number of days to query (1-10, default: 1)
        date: Optional date in YYYY-MM-DD format. If None, returns most recent data
    """
    logger.info("sentinel_dispatch.services.ingest_fire.fetch_and_publish_fires.fetching", source=source, days=days, bbox=bbox)

    # If date is provided and it's in the past, consider using SP (Standard Processing) sources
    # SP sources have more complete historical data
    if date:
        from datetime import datetime, UTC
        try:
            query_date = datetime.strptime(date, "%Y-%m-%d")
            # If date is more than 7 days ago, use SP source for better historical coverage
            days_ago = (datetime.now(UTC) - query_date.replace(tzinfo=UTC)).days
            if days_ago > 7:
                source = "VIIRS_SNPP_SP"  # Standard Processing for historical data
                logger.info("sentinel_dispatch.services.ingest_fire.main.using_sp_source",
                            date=date, days_ago=days_ago, source=source)
        except ValueError:
            logger.warning("sentinel_dispatch.services.ingest_fire.main.invalid_date_format", date=date)

    fires = client.get_fire_data(source=source, days=days, bbox=bbox, date=date)

    if not fires:
        logger.warning("sentinel_dispatch.services.ingest_fire.fetch_and_publish_fires.no_data")
        return

    published = 0
    for fire_data in fires:
        try:
            message = format_fire_message(fire_data)
            fire_id = message["fireId"]
            producer.publish(topic, key=fire_id, value=message)
            published += 1
        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest_fire.fetch_and_publish_fires.processing_error", error=str(e), exc_info=True)

    producer.flush()
    logger.info("sentinel_dispatch.services.ingest_fire.fetch_and_publish_fires.published", count=published, topic=topic)


def main():
    """Main entry point for fire ingestion service."""
    parser = argparse.ArgumentParser(description="Ingest fire data from FIRMS API and publish to Kafka")
    parser.add_argument(
        "--topic",
        type=str,
        default="fire-data",
        help="Kafka topic name (default: fire-data)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Polling interval in seconds (default: 3600 = 1 hour)",
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

        # Get bounding box and event date from config
        location = config.location
        bbox = location.get("bounding_box", {})
        if bbox:
            logger.info("sentinel_dispatch.services.ingest_fire.main.using_bbox", bbox=bbox)

        # Get event date from config (for historical data)
        date = location.get("event_date")  # Optional, if None uses most recent data
        if date:
            logger.info("sentinel_dispatch.services.ingest_fire.main.using_config_date", date=date)

        # Get FIRMS API config
        firms_config = config.apis.get("firms", {})
        base_url = firms_config.get("base_url", "https://firms.modaps.eosdis.nasa.gov/api/")
        map_key = firms_config.get("api_key")  # MAP_KEY is stored as api_key in config

        if not map_key:
            logger.error("sentinel_dispatch.services.ingest_fire.main.no_map_key",
                        message="FIRMS_API_KEY (MAP_KEY) is required. Get a free key from https://firms.modaps.eosdis.nasa.gov/api/")
            sys.exit(1)

        # Ensure topic exists
        logger.info("sentinel_dispatch.services.ingest_fire.main.ensuring_topic", topic=args.topic)
        ensure_topic_exists(bootstrap_servers, args.topic)

        # Create producer and client
        producer = KafkaProducer(bootstrap_servers)
        client = FIRMSClient(base_url, map_key)

        days = 1  # Last 24 hours

        if args.once:
            # Fetch once and exit (use date from config if available)
            fetch_and_publish_fires(producer, client, bbox, args.topic, days=days, date=date)
        else:
            # Continuous polling (use most recent data, ignore config date)
            if date:
                logger.warning("sentinel_dispatch.services.ingest_fire.main.date_ignored_in_continuous",
                             message="Config event_date is ignored in continuous mode. Using most recent data.")
            logger.info("sentinel_dispatch.services.ingest_fire.main.starting_continuous", interval=args.interval)
            while True:
                try:
                    # Always use most recent data in continuous mode
                    fetch_and_publish_fires(producer, client, bbox, args.topic, days=days, date=None)
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    logger.info("sentinel_dispatch.services.ingest_fire.main.interrupted")
                    break
                except Exception as e:
                    logger.error("sentinel_dispatch.services.ingest_fire.main.loop_error", error=str(e), exc_info=True)
                    time.sleep(300)  # Wait 5 minutes before retrying

    except KeyboardInterrupt:
        logger.info("sentinel_dispatch.services.ingest_fire.main.interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error("sentinel_dispatch.services.ingest_fire.main.fatal_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

