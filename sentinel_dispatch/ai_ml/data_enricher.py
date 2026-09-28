"""
Data Enrichment Module for Emergency Dispatch.

This module enriches processed emergency calls with contextual data from weather
and fire sources using spatial-temporal joins.

Features:
- Spatial-temporal joins with weather data (nearest station, time window)
- Fire proximity analysis (distance, bearing, spread direction)
- Wind direction impact analysis
- Handles missing data gracefully
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sentinel_dispatch.ai_ml.models import (
    ProcessedCall,
    EnrichedCall,
    FireProximityAnalysis,
    WeatherEnrichment,
)
from sentinel_dispatch.utils.geospatial import haversine_distance, bearing
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)


class DataEnricher:
    """Enriches processed calls with weather and fire data."""
    
    def __init__(
        self,
        weather_time_window_minutes: Optional[int] = None,  # Auto-detect: 15 min for real-time, 1 day for historical
        fire_time_window_hours: int = 3,
        max_weather_distance_km: float = 50.0,
        historical_threshold_hours: int = 24,  # Calls older than this are considered historical
    ):
        """
        Initialize the data enricher.
        
        Args:
            weather_time_window_minutes: Time window for weather data join. If None, auto-detects:
                                        - 15 minutes for real-time calls (within 24 hours)
                                        - 1440 minutes (1 day) for historical calls (older than 24 hours)
            fire_time_window_hours: Time window for fire data join (default: 3 hours)
            max_weather_distance_km: Maximum distance to consider weather station (default: 50 km)
            historical_threshold_hours: Calls older than this many hours are considered historical (default: 24)
        """
        self.weather_time_window_minutes = weather_time_window_minutes
        self.fire_time_window_hours = fire_time_window_hours
        self.max_weather_distance_km = max_weather_distance_km
        self.historical_threshold_hours = historical_threshold_hours
        
        logger.info(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher.__init__.initialized",
            weather_time_window_minutes=weather_time_window_minutes,
            fire_time_window_hours=fire_time_window_hours,
            max_weather_distance_km=max_weather_distance_km,
            historical_threshold_hours=historical_threshold_hours,
            note="Weather time window will be auto-detected based on call timestamp if not explicitly set"
        )
    
    
    def _find_nearest_weather(
        self,
        caller_location: Dict[str, float],
        call_timestamp: str,
        weather_data_list: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Find nearest weather station within time window.
        
        Args:
            caller_location: Caller location {"lat": float, "lon": float}
            call_timestamp: Call timestamp (ISO format)
            weather_data_list: List of weather data records
            
        Returns:
            Nearest weather data record or None
        """
        if not weather_data_list:
            logger.debug(
                "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.empty_list"
            )
            return None
        
        caller_lat = caller_location["lat"]
        caller_lon = caller_location["lon"]
        
        try:
            call_time = datetime.fromisoformat(call_timestamp.replace("Z", "+00:00"))
        except Exception:
            logger.warning(
                "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.invalid_timestamp",
                call_timestamp=call_timestamp
            )
            return None
        
        # Determine if this is a historical call and adjust time window accordingly
        now = datetime.now(timezone.utc)
        call_age_hours = abs((now - call_time).total_seconds() / 3600.0)
        is_historical = call_age_hours > self.historical_threshold_hours
        
        # Auto-detect time window: 15 minutes for real-time, 1 day (1440 min) for historical
        if self.weather_time_window_minutes is None:
            weather_window = 1440 if is_historical else 15  # 1 day for historical, 15 min for real-time
        else:
            weather_window = self.weather_time_window_minutes
        
        logger.debug(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.starting",
            weather_count=len(weather_data_list),
            caller_location=caller_location,
            call_timestamp=call_timestamp,
            call_time=call_time.isoformat(),
            call_age_hours=call_age_hours,
            is_historical=is_historical,
            time_window_minutes=weather_window,
            note="Time window is relative to call timestamp, not current time"
        )
        
        nearest = None
        min_distance = float('inf')
        checked_count = 0
        skipped_time = 0
        skipped_distance = 0
        skipped_format = 0
        
        for weather in weather_data_list:
            checked_count += 1
            # Check time window - relative to call timestamp, not current time
            # This allows matching historical calls with historical weather data
            # For historical data, timestamp may be null (meaning it matches the call date)
            weather_timestamp = weather.get("timestamp")
            if weather_timestamp is None or weather_timestamp == "":
                # Null/empty timestamp for historical data - accept it (skip time window check)
                # This is valid for historical weather data that matches the call date
                logger.debug(
                    "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.null_timestamp",
                    note="Accepting weather data with null timestamp (historical data matching call date)"
                )
                # Continue to distance check (skip time window validation)
            else:
                # Timestamp is present - validate time window
                try:
                    weather_time = datetime.fromisoformat(
                        str(weather_timestamp).replace("Z", "+00:00")
                    )
                    # Calculate time difference: weather can be before or after call
                    # Use absolute difference to allow weather slightly before or after the call
                    time_diff_minutes = abs((call_time - weather_time).total_seconds() / 60.0)
                    if time_diff_minutes > weather_window:
                        skipped_time += 1
                        continue
                except Exception as e:
                    # Skip if timestamp is invalid (but not null)
                    skipped_format += 1
                    logger.debug(
                        "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.invalid_timestamp",
                        error=str(e),
                        weather_data_keys=list(weather.keys()),
                        timestamp_value=weather_timestamp
                    )
                    continue
            
            # Check distance
            weather_location = weather.get("location", {})
            weather_lat = weather_location.get("lat")
            weather_lon = weather_location.get("lon")
            
            if weather_lat is None or weather_lon is None:
                skipped_format += 1
                logger.debug(
                    "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.missing_location",
                    weather_data_keys=list(weather.keys()),
                    has_location="location" in weather
                )
                continue
            
            distance = haversine_distance(
                caller_lat, caller_lon, weather_lat, weather_lon
            )
            
            if distance > self.max_weather_distance_km:
                skipped_distance += 1
                continue
            
            if distance < min_distance:
                min_distance = distance
                nearest = weather.copy()
                nearest["_distance_km"] = distance
        
        logger.info(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_weather.completed",
            checked_count=checked_count,
            skipped_time=skipped_time,
            skipped_distance=skipped_distance,
            skipped_format=skipped_format,
            found=nearest is not None,
            min_distance=min_distance if nearest else None,
            time_window_minutes=weather_window,
            is_historical=is_historical
        )
        
        return nearest
    
    def _find_nearest_fire(
        self,
        caller_location: Dict[str, float],
        call_timestamp: str,
        fire_data_list: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Find nearest fire within time window.
        
        Args:
            caller_location: Caller location {"lat": float, "lon": float}
            call_timestamp: Call timestamp (ISO format)
            fire_data_list: List of fire data records
            
        Returns:
            Nearest fire data record with distance and bearing, or None
        """
        if not fire_data_list:
            logger.debug(
                "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.empty_list"
            )
            return None
        
        caller_lat = caller_location["lat"]
        caller_lon = caller_location["lon"]
        
        try:
            call_time = datetime.fromisoformat(call_timestamp.replace("Z", "+00:00"))
        except Exception:
            logger.warning(
                "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.invalid_timestamp",
                call_timestamp=call_timestamp
            )
            return None
        
        # Determine if this is a historical call and adjust time window accordingly
        # Fires can persist for days, so use a longer window for historical calls
        now = datetime.now(timezone.utc)
        call_age_hours = abs((now - call_time).total_seconds() / 3600.0)
        is_historical = call_age_hours > self.historical_threshold_hours
        
        # Auto-adjust time window: 3 hours for real-time, 24 hours (1 day) for historical
        # Historical fires might have been detected hours or days before the call
        fire_window_hours = 24 if is_historical else self.fire_time_window_hours
        
        logger.debug(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.starting",
            fire_count=len(fire_data_list),
            caller_location=caller_location,
            call_timestamp=call_timestamp,
            call_time=call_time.isoformat(),
            call_age_hours=call_age_hours,
            is_historical=is_historical,
            time_window_hours=fire_window_hours,
            note="Time window is relative to call timestamp, not current time"
        )
        
        nearest = None
        min_distance = float('inf')
        checked_count = 0
        skipped_time = 0
        skipped_format = 0
        
        for fire in fire_data_list:
            checked_count += 1
            # Check time window - relative to call timestamp, not current time
            # This allows matching historical calls with historical fire data
            # Fires can be older than the call (they started before), so use hours
            # For historical data, timestamp may be null (meaning it matches the call date)
            fire_timestamp = fire.get("timestamp")
            if fire_timestamp is None or fire_timestamp == "":
                # Null/empty timestamp for historical data - accept it (skip time window check)
                # This is valid for historical fire data that matches the call date
                logger.debug(
                    "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.null_timestamp",
                    note="Accepting fire data with null timestamp (historical data matching call date)"
                )
                # Continue to distance check (skip time window validation)
            else:
                # Timestamp is present - validate time window
                try:
                    fire_time = datetime.fromisoformat(
                        str(fire_timestamp).replace("Z", "+00:00")
                    )
                    # Calculate time difference: fire can be before or after call
                    # Use absolute difference, but fires are typically detected before calls
                    time_diff_hours = abs((call_time - fire_time).total_seconds() / 3600.0)
                    if time_diff_hours > fire_window_hours:
                        skipped_time += 1
                        continue
                except Exception as e:
                    # Skip if timestamp is invalid (but not null)
                    skipped_format += 1
                    logger.debug(
                        "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.invalid_timestamp",
                        error=str(e),
                        fire_data_keys=list(fire.keys())
                    )
                    continue
            
            # Check distance
            fire_location = fire.get("location", {})
            fire_lat = fire_location.get("lat")
            fire_lon = fire_location.get("lon")
            
            if fire_lat is None or fire_lon is None:
                skipped_format += 1
                logger.debug(
                    "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.missing_location",
                    fire_data_keys=list(fire.keys()),
                    has_location="location" in fire
                )
                continue
            
            distance = haversine_distance(
                caller_lat, caller_lon, fire_lat, fire_lon
            )
            
            if distance < min_distance:
                min_distance = distance
                nearest = fire.copy()
                nearest["_distance_km"] = distance
                nearest["_bearing_degrees"] = bearing(
                    caller_lat, caller_lon, fire_lat, fire_lon
                )
        
        logger.info(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._find_nearest_fire.completed",
            checked_count=checked_count,
            skipped_time=skipped_time,
            skipped_format=skipped_format,
            found=nearest is not None,
            min_distance=min_distance if nearest else None
        )
        
        return nearest
    
    def _analyze_fire_proximity(
        self,
        caller_location: Dict[str, float],
        nearest_fire: Optional[Dict[str, Any]],
        weather_enrichment: Optional[WeatherEnrichment],
    ) -> Optional[FireProximityAnalysis]:
        """
        Analyze fire proximity and spread direction.
        
        Args:
            caller_location: Caller location {"lat": float, "lon": float}
            nearest_fire: Nearest fire data with distance and bearing
            weather_enrichment: Weather enrichment data (for wind direction)
            
        Returns:
            FireProximityAnalysis or None
        """
        if not nearest_fire:
            return None
        
        fire_location = nearest_fire.get("location", {})
        fire_lat = fire_location.get("lat")
        fire_lon = fire_location.get("lon")
        
        if fire_lat is None or fire_lon is None:
            return None
        
        distance_km = nearest_fire.get("_distance_km")
        bearing_to_fire = nearest_fire.get("_bearing_degrees")
        spread_rate = nearest_fire.get("spreadRate") or nearest_fire.get("spread_rate")
        fire_id = nearest_fire.get("fireId") or nearest_fire.get("fire_id")
        
        # Calculate if fire is spreading toward caller
        spread_toward_caller = None
        estimated_time_to_reach = None
        
        if spread_rate and spread_rate > 0 and distance_km:
            # Get wind direction if available
            wind_direction = None
            if weather_enrichment and weather_enrichment.wind_direction is not None:
                wind_direction = weather_enrichment.wind_direction
            
            # Simple heuristic: if wind is blowing from fire toward caller, fire is spreading that way
            # More sophisticated: calculate if fire spread direction (based on wind) aligns with bearing to caller
            if wind_direction is not None:
                # Wind direction is where wind is coming FROM
                # Fire typically spreads in the direction wind is blowing TO
                wind_to_direction = (wind_direction + 180) % 360
                
                # Check if wind direction aligns with bearing to caller (within 45 degrees)
                angle_diff = abs((wind_to_direction - bearing_to_fire + 180) % 360 - 180)
                if angle_diff <= 45:
                    spread_toward_caller = True
                    # Estimate time to reach: distance / spread_rate
                    estimated_time_to_reach = distance_km / spread_rate
                else:
                    spread_toward_caller = False
            else:
                # Without wind data, assume fire spreads in all directions
                # Estimate time based on distance and spread rate
                spread_toward_caller = None
                estimated_time_to_reach = distance_km / spread_rate if spread_rate > 0 else None
        
        # Extract FRP from original fire data
        frp = nearest_fire.get("frp") or nearest_fire.get("FRP")
        
        return FireProximityAnalysis(
            nearest_fire_id=fire_id,
            distance_km=distance_km,
            bearing_degrees=bearing_to_fire,
            spread_rate=spread_rate,
            spread_toward_caller=spread_toward_caller,
            estimated_time_to_reach=estimated_time_to_reach,
            fire_location={"lat": fire_lat, "lon": fire_lon},
            frp=frp,
        )
    
    def _enrich_weather(
        self,
        caller_location: Dict[str, float],
        nearest_fire: Optional[Dict[str, Any]],
        weather_data: Optional[Dict[str, Any]],
    ) -> Optional[WeatherEnrichment]:
        """
        Create weather enrichment from weather data.
        
        Args:
            caller_location: Caller location {"lat": float, "lon": float}
            nearest_fire: Nearest fire data (for wind direction analysis)
            weather_data: Weather data record
            
        Returns:
            WeatherEnrichment or None
        """
        if not weather_data:
            return None
        
        weather_location = weather_data.get("location", {})
        weather_lat = weather_location.get("lat")
        weather_lon = weather_location.get("lon")
        
        distance_km = weather_data.get("_distance_km")
        
        # Convert wind direction from string to degrees if needed
        wind_direction_raw = weather_data.get("windDirection") or weather_data.get("wind_direction")
        wind_direction = None
        if wind_direction_raw is not None:
            if isinstance(wind_direction_raw, (int, float)):
                # Already a number
                wind_direction = float(wind_direction_raw)
            elif isinstance(wind_direction_raw, str):
                # Convert cardinal/compass directions to degrees
                wind_dir_upper = wind_direction_raw.strip().upper()
                wind_direction_map = {
                    "N": 0.0, "NNE": 22.5, "NE": 45.0, "ENE": 67.5,
                    "E": 90.0, "ESE": 112.5, "SE": 135.0, "SSE": 157.5,
                    "S": 180.0, "SSW": 202.5, "SW": 225.0, "WSW": 247.5,
                    "W": 270.0, "WNW": 292.5, "NW": 315.0, "NNW": 337.5,
                }
                if wind_dir_upper in wind_direction_map:
                    wind_direction = wind_direction_map[wind_dir_upper]
                else:
                    # Try to parse as number string
                    try:
                        wind_direction = float(wind_direction_raw)
                    except (ValueError, TypeError):
                        logger.debug(
                            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._enrich_weather.invalid_wind_direction",
                            wind_direction=wind_direction_raw,
                            note="Could not parse wind direction, setting to None"
                        )
                        wind_direction = None
        
        # Handle visibility - convert empty string to None
        visibility_raw = weather_data.get("visibility")
        visibility = None
        if visibility_raw is not None:
            if isinstance(visibility_raw, (int, float)):
                visibility = float(visibility_raw)
            elif isinstance(visibility_raw, str):
                visibility_str = visibility_raw.strip()
                if visibility_str and visibility_str != "":
                    try:
                        visibility = float(visibility_str)
                    except (ValueError, TypeError):
                        logger.debug(
                            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher._enrich_weather.invalid_visibility",
                            visibility=visibility_raw,
                            note="Could not parse visibility, setting to None"
                        )
                        visibility = None
                # Empty string -> None (already set)
        
        # Analyze wind direction relative to fire
        wind_toward_fire = None
        if nearest_fire and wind_direction is not None:
            fire_location = nearest_fire.get("location", {})
            fire_lat = fire_location.get("lat")
            fire_lon = fire_location.get("lon")
            
            if fire_lat is not None and fire_lon is not None:
                # Calculate bearing from caller to fire
                bearing_to_fire = bearing(
                    caller_location["lat"],
                    caller_location["lon"],
                    fire_lat,
                    fire_lon,
                )
                
                # Wind direction is where wind is coming FROM
                # Check if wind is blowing toward fire (within 45 degrees)
                wind_from_direction = wind_direction
                angle_diff = abs((wind_from_direction - bearing_to_fire + 180) % 360 - 180)
                wind_toward_fire = angle_diff <= 45
        
        return WeatherEnrichment(
            temperature=weather_data.get("temperature"),
            wind_speed=weather_data.get("windSpeed") or weather_data.get("wind_speed"),
            wind_direction=wind_direction,  # Already converted to float or None
            humidity=weather_data.get("humidity"),
            air_quality_index=weather_data.get("airQualityIndex") or weather_data.get("air_quality_index"),
            visibility=visibility,  # Already converted to float or None
            weather_location={"lat": weather_lat, "lon": weather_lon} if weather_lat and weather_lon else None,
            weather_distance_km=distance_km,
            wind_toward_fire=wind_toward_fire,
        )
    
    def enrich_call(
        self,
        processed_call: ProcessedCall,
        weather_data_list: Optional[List[Dict[str, Any]]] = None,
        fire_data_list: Optional[List[Dict[str, Any]]] = None,
    ) -> EnrichedCall:
        """
        Enrich a processed call with weather and fire data.
        
        Args:
            processed_call: Processed call with classification
            weather_data_list: List of weather data records (optional)
            fire_data_list: List of fire data records (optional)
            
        Returns:
            Enriched call with weather and fire analysis
        """
        logger.debug(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher.enrich_call.starting",
            call_id=processed_call.call_id
        )
        
        weather_data_list = weather_data_list or []
        fire_data_list = fire_data_list or []
        
        # Find nearest weather station
        nearest_weather = self._find_nearest_weather(
            processed_call.caller_location,
            processed_call.timestamp,
            weather_data_list,
        )
        
        # Find nearest fire
        nearest_fire = self._find_nearest_fire(
            processed_call.caller_location,
            processed_call.timestamp,
            fire_data_list,
        )
        
        # Create weather enrichment
        weather_enrichment = self._enrich_weather(
            processed_call.caller_location,
            nearest_fire,
            nearest_weather,
        )
        
        # Analyze fire proximity
        fire_analysis = self._analyze_fire_proximity(
            processed_call.caller_location,
            nearest_fire,
            weather_enrichment,
        )
        
        # Create enriched call
        enriched_call = EnrichedCall(
            call_id=processed_call.call_id,
            timestamp=processed_call.timestamp,
            caller_location=processed_call.caller_location,
            transcript=processed_call.transcript,
            classification=processed_call.classification,
            metadata=processed_call.metadata,
            fire_analysis=fire_analysis,
            weather_enrichment=weather_enrichment,
            enrichment_timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
        logger.info(
            "sentinel_dispatch.ai_ml.data_enricher.DataEnricher.enrich_call.completed",
            call_id=processed_call.call_id,
            has_fire_analysis=fire_analysis is not None,
            has_weather_enrichment=weather_enrichment is not None
        )
        
        return enriched_call


# Convenience function
def enrich_call(
    processed_call: ProcessedCall,
    weather_data_list: Optional[List[Dict[str, Any]]] = None,
    fire_data_list: Optional[List[Dict[str, Any]]] = None,
) -> EnrichedCall:
    """
    Convenience function to enrich a processed call.
    
    Args:
        processed_call: Processed call with classification
        weather_data_list: List of weather data records (optional)
        fire_data_list: List of fire data records (optional)
        
    Returns:
        Enriched call with weather and fire analysis
    """
    enricher = DataEnricher()
    return enricher.enrich_call(processed_call, weather_data_list, fire_data_list)

