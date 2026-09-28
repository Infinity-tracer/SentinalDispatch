#!/usr/bin/env python3
"""Centralized Data Ingestion Service - Orchestrates all ingestion services."""
import argparse
import os
import sys
import time
from typing import Optional, Any, Dict

from sentinel_dispatch.utils.config import load_config
from sentinel_dispatch.utils.kafka_utils import ensure_topic_exists, KafkaProducer
from sentinel_dispatch.utils.logging_config import configure_logging, get_logger

from sentinel_dispatch.services.ingest_calls import ingest_calls_from_file
from sentinel_dispatch.services.ingest_weather import (
    fetch_and_publish_weather,
    NOAAWeatherClient,
)
from sentinel_dispatch.services.ingest_fire import (
    fetch_and_publish_fires,
    FIRMSClient,
)

# Configure logging
configure_logging()
logger = get_logger(__name__)


class IngestionOrchestrator:
    """Orchestrates all data ingestion services."""

    def __init__(self):
        """
        Initialize ingestion orchestrator.

        Mode is read from SENTINEL_MODE environment variable.
        """
        self.mode = os.getenv("SENTINEL_MODE", "development")
        if self.mode not in ["development", "production"]:
            raise ValueError(f"Invalid SENTINEL_MODE: {self.mode}. Must be 'development' or 'production'")
        self.config = load_config()
        self.kafka_config = self.config.kafka
        self.bootstrap_servers = self.kafka_config.get("bootstrap_servers", "localhost:9092")

        # Initialize Kafka producers
        self.calls_producer = KafkaProducer(self.bootstrap_servers)
        self.weather_producer = KafkaProducer(self.bootstrap_servers)
        self.fire_producer = KafkaProducer(self.bootstrap_servers)

        # Initialize API clients
        noaa_config = self.config.apis.get("noaa", {})
        self.weather_client = NOAAWeatherClient(
            noaa_config.get("base_url", "https://api.weather.gov/"),
            noaa_config.get("user_agent", "SentinelDispatch/1.0"),
        )

        firms_config = self.config.apis.get("firms", {})
        # FIRMS API requires MAP_KEY (stored as api_key in config)
        self.fire_client = FIRMSClient(
            firms_config.get("base_url", "https://firms.modaps.eosdis.nasa.gov/api/"),
            firms_config.get("api_key"),  # This is the MAP_KEY
        )

    def ensure_topics(self) -> None:
        """Ensure all required Kafka topics exist."""
        topics = ["emergency-calls", "weather-data", "fire-data"]
        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ensure_topics.starting")
        for topic in topics:
            ensure_topic_exists(self.bootstrap_servers, topic)
        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ensure_topics.completed")

    def ingest_calls(self, file_path: str) -> bool:
        """
        Ingest emergency calls from a file.

        Args:
            file_path: Path to JSON file containing calls

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_calls.starting", file_path=file_path)
            ingest_calls_from_file(self.calls_producer, file_path, "emergency-calls")
            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_calls.completed")
            return True
        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_calls.failed", error=str(e), exc_info=True)
            return False

    def ingest_weather(self, location: Optional[dict] = None) -> bool:
        """
        Ingest weather data.
        Uses event_date from config.location if available for historical data.

        Args:
            location: Optional location dict with lat/lon. If None, uses default from config.

        Returns:
            True if successful, False otherwise
        """
        try:
            if location is None:
                # Use default location from config (Maui island)
                location = self.config.location
                bbox = location.get("bounding_box", {})
                location = {
                    "lat": (bbox.get("north", 20.90) + bbox.get("south", 20.85)) / 2,
                    "lon": (bbox.get("east", -156.65) + bbox.get("west", -156.70)) / 2,
                }
                logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_weather.using_default_location", location=location)

            # Get event_date from config for historical data
            event_date = self.config.location.get("event_date")

            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_weather.starting", date=event_date)
            fetch_and_publish_weather(self.weather_producer, self.weather_client, location, "weather-data", date=event_date)
            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_weather.completed")
            return True
        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_weather.failed", error=str(e), exc_info=True)
            return False

    def ingest_fire(self) -> bool:
        """
        Ingest fire data.
        Uses event_date from config.location if available, otherwise uses most recent data.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get bounding box and event date from config
            location = self.config.location
            bbox = location.get("bounding_box", {})
            date = location.get("event_date")  # Optional, from config

            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_fire.starting", date=date)
            fetch_and_publish_fires(
                self.fire_producer,
                self.fire_client,
                bbox,
                topic="fire-data",
                days=1,
                date=date,
            )
            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_fire.completed")
            return True
        except Exception as e:
            logger.error("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_fire.failed", error=str(e), exc_info=True)
            return False

    def ingest_all(self, calls_file: Optional[str] = None, location: Optional[dict] = None) -> dict:
        """
        Run all ingestion services sequentially.

        Args:
            calls_file: Optional path to calls file. If None, skips call ingestion.
            location: Optional location dict with lat/lon for weather. If None, uses default from config.

        Returns:
            Dictionary with results for each service
        """
        results: Dict[str, Any] = {
            "calls": None,
            "weather": None,
            "fire": None,
        }

        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_all.starting")

        # Ensure topics exist first
        self.ensure_topics()

        # Ingest calls (if file provided)
        if calls_file:
            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_all.ingesting_calls")
            results["calls"] = self.ingest_calls(calls_file)
        else:
            logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_all.skipping_calls")
            results["calls"] = None

        # Ingest weather
        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_all.ingesting_weather")
        results["weather"] = self.ingest_weather(location)

        # Ingest fire (uses event_date from config if available)
        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_all.ingesting_fire")
        results["fire"] = self.ingest_fire()

        # Summary
        logger.info(
            "sentinel_dispatch.services.ingest.IngestionOrchestrator.ingest_all.summary",
            calls=results['calls'],
            weather=results['weather'],
            fire=results['fire'],
        )

        return results

    def run_continuous(self, calls_file: Optional[str] = None, interval: int = 900) -> None:
        """
        Run continuous ingestion loop.

        Args:
            calls_file: Optional path to calls file (only ingested once at start)
            interval: Polling interval in seconds (default: 900 = 15 minutes)
        """
        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.run_continuous.starting", interval=interval)
        logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.run_continuous.ready")

        # Initial ingestion (including calls if file provided)
        # Note: For fire data, initial ingestion uses event_date from config if available
        self.ingest_all(calls_file)

        # Continuous loop for weather and fire
        # Note: In continuous mode, fire data always uses most recent data (ignores config event_date)
        while True:
            try:
                time.sleep(interval)
                logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.run_continuous.cycle_starting")
                # Temporarily override config event_date to None for continuous mode
                original_date = self.config.location.get("event_date")
                if original_date:
                    # Temporarily remove event_date for this cycle
                    self.config.location.pop("event_date", None)
                try:
                    self.ingest_all(calls_file=None)  # Don't re-ingest calls
                finally:
                    # Restore original date if it existed
                    if original_date:
                        self.config.location["event_date"] = original_date
            except KeyboardInterrupt:
                logger.info("sentinel_dispatch.services.ingest.IngestionOrchestrator.run_continuous.stopping")
                break
            except Exception as e:
                logger.error("sentinel_dispatch.services.ingest.IngestionOrchestrator.run_continuous.cycle_error", error=str(e), exc_info=True)
                time.sleep(60)  # Wait before retrying


def main():
    """Main entry point for centralized ingestion service."""
    parser = argparse.ArgumentParser(
        description="Centralized data ingestion service - orchestrates all ingestion services"
    )
    parser.add_argument(
        "--mode",
        type=str,
        default="development",
        choices=["development", "production"],
        help="Mode to run in (default: development)",
    )
    parser.add_argument(
        "--calls-file",
        type=str,
        help="Path to JSON file containing calls to ingest",
    )
    parser.add_argument(
        "--weather-only",
        action="store_true",
        help="Only ingest weather data",
    )
    parser.add_argument(
        "--fire-only",
        action="store_true",
        help="Only ingest fire data",
    )
    parser.add_argument(
        "--calls-only",
        action="store_true",
        help="Only ingest calls (requires --calls-file)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuous ingestion loop",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=900,
        help="Polling interval in seconds for continuous mode (default: 900 = 15 minutes)",
    )
    parser.add_argument(
        "--lat",
        type=float,
        help="Latitude for weather data (overrides config default)",
    )
    parser.add_argument(
        "--lon",
        type=float,
        help="Longitude for weather data (overrides config default)",
    )

    args = parser.parse_args()

    try:
        orchestrator = IngestionOrchestrator()

        if args.calls_only:
            if not args.calls_file:
                logger.error("sentinel_dispatch.services.ingest.main.calls_only_requires_file", calls_only=True)
                sys.exit(1)
            orchestrator.ensure_topics()
            success = orchestrator.ingest_calls(args.calls_file)
            sys.exit(0 if success else 1)

        elif args.weather_only:
            orchestrator.ensure_topics()
            location = None
            if args.lat and args.lon:
                location = {"lat": args.lat, "lon": args.lon}
            success = orchestrator.ingest_weather(location)
            sys.exit(0 if success else 1)

        elif args.fire_only:
            orchestrator.ensure_topics()
            success = orchestrator.ingest_fire()
            sys.exit(0 if success else 1)

        elif args.continuous:
            orchestrator.run_continuous(args.calls_file, args.interval)

        else:
            # Run all services
            location = None
            if args.lat and args.lon:
                location = {"lat": args.lat, "lon": args.lon}
            
            # Run all services with optional custom location
            # Fire data uses event_date from config if available
            results = orchestrator.ingest_all(args.calls_file, location)

            # Exit with error if any required service failed
            if results["weather"] is False or results["fire"] is False:
                sys.exit(1)

    except KeyboardInterrupt:
        logger.info("sentinel_dispatch.services.ingest.main.interrupted")
        sys.exit(0)
    except Exception as e:
        logger.error("sentinel_dispatch.services.ingest.main.fatal_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

