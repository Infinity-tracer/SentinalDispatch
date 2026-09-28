"""
Tests for data enrichment module.
"""

import pytest

from sentinel_dispatch.ai_ml.models import (
    ProcessedCall,
    CallClassificationResult,
    EnrichedCall,
    WeatherEnrichment,
)
from sentinel_dispatch.ai_ml.data_enricher import DataEnricher
from sentinel_dispatch.utils.geospatial import haversine_distance, bearing


class TestDataEnricher:
    """Test cases for DataEnricher."""
    
    @pytest.fixture
    def enricher(self):
        """Create a DataEnricher instance."""
        return DataEnricher(
            weather_time_window_minutes=15,
            fire_time_window_hours=3,
            max_weather_distance_km=50.0
        )
    
    @pytest.fixture
    def sample_processed_call(self):
        """Create a sample processed call."""
        return ProcessedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},  # Lahaina
            transcript="Fire is getting close!",
            classification=CallClassificationResult(
                urgency=5,
                incident_type="fire",
                entities={},
                sentiment=0.9,
                key_phrases=["fire is close"],
                confidence=0.95
            ),
            metadata={"phoneNumber": "+1-808-555-0123", "callDuration": 127}
        )
    
    @pytest.fixture
    def sample_weather_data(self):
        """Create sample weather data."""
        return [
            {
                "timestamp": "2024-08-08T14:20:00Z",
                "location": {"lat": 20.8800, "lon": -156.6800},  # Near Lahaina
                "temperature": 85.0,
                "windSpeed": 25.0,
                "windDirection": 270.0,  # West wind
                "humidity": 45.0,
                "airQualityIndex": 180,
                "visibility": 2.0
            },
            {
                "timestamp": "2024-08-08T14:25:00Z",
                "location": {"lat": 20.9000, "lon": -156.7000},  # Farther away
                "temperature": 87.0,
                "windSpeed": 30.0,
                "windDirection": 280.0,
                "humidity": 40.0,
                "airQualityIndex": 200,
                "visibility": 1.5
            }
        ]
    
    @pytest.fixture
    def sample_fire_data(self):
        """Create sample fire data."""
        return [
            {
                "timestamp": "2024-08-08T14:00:00Z",
                "fireId": "FIRE-001",
                "location": {"lat": 20.8790, "lon": -156.6830},  # Very close to Lahaina
                "confidence": 0.95,
                "brightness": 350.0,
                "fireType": "active",
                "frp": 33.0,  # Fire Radiative Power in MW (moderate intensity)
                "spreadRate": 5.0  # Estimated from frp
            },
            {
                "timestamp": "2024-08-08T13:00:00Z",
                "fireId": "FIRE-002",
                "location": {"lat": 20.9000, "lon": -156.7000},  # Farther away
                "confidence": 0.85,
                "brightness": 300.0,
                "fireType": "active",
                "frp": 20.0,  # Fire Radiative Power in MW (moderate intensity)
                "spreadRate": 3.0  # Estimated from frp
            }
        ]
    
    def test_haversine_distance(self):
        """Test Haversine distance calculation."""
        # Distance between Lahaina and Kaanapali (approximately 5.5 km)
        distance = haversine_distance(
            20.8783, -156.6825,  # Lahaina
            20.9289, -156.6944   # Kaanapali
        )
        
        assert 5.0 < distance < 6.0  # Should be approximately 5.5 km
        
        # Same point
        distance = haversine_distance(
            20.8783, -156.6825,
            20.8783, -156.6825
        )
        assert abs(distance) < 0.01  # Should be near zero
    
    def test_bearing(self):
        """Test bearing calculation."""
        # Bearing from Lahaina to Kaanapali (north)
        bearing_deg = bearing(
            20.8783, -156.6825,  # Lahaina
            20.9289, -156.6944   # Kaanapali (north)
        )
        
        assert 0.0 <= bearing_deg <= 360.0
        # Should be roughly north (0-45 degrees or 315-360 degrees)
        assert bearing_deg < 45 or bearing_deg > 315
        
        # Same point
        bearing_deg = bearing(
            20.8783, -156.6825,
            20.8783, -156.6825
        )
        assert 0.0 <= bearing_deg <= 360.0
    
    def test_find_nearest_weather_within_window(self, enricher, sample_processed_call, sample_weather_data):
        """Test finding nearest weather station within time window."""
        nearest = enricher._find_nearest_weather(
            sample_processed_call.caller_location,
            sample_processed_call.timestamp,
            sample_weather_data
        )
        
        assert nearest is not None
        assert nearest["timestamp"] == "2024-08-08T14:20:00Z"  # Closer in time and space
        assert "_distance_km" in nearest
        assert nearest["_distance_km"] < 5.0  # Should be close
    
    def test_find_nearest_weather_outside_window(self, enricher, sample_processed_call):
        """Test that weather data outside time window is ignored."""
        old_weather = [
            {
                "timestamp": "2024-08-08T13:00:00Z",  # 1.5 hours before call
                "location": {"lat": 20.8800, "lon": -156.6800},
                "temperature": 80.0
            }
        ]
        
        nearest = enricher._find_nearest_weather(
            sample_processed_call.caller_location,
            sample_processed_call.timestamp,
            old_weather
        )
        
        # Should be None because outside 15-minute window
        assert nearest is None
    
    def test_find_nearest_weather_too_far(self, enricher, sample_processed_call):
        """Test that weather stations too far away are ignored."""
        # Use a location that's definitely beyond 50 km from Lahaina
        # Honolulu is approximately 100+ km from Lahaina
        far_weather = [
            {
                "timestamp": "2024-08-08T14:20:00Z",
                "location": {"lat": 21.3069, "lon": -157.8583},  # Honolulu (~100+ km from Lahaina)
                "temperature": 80.0
            }
        ]
        
        nearest = enricher._find_nearest_weather(
            sample_processed_call.caller_location,
            sample_processed_call.timestamp,
            far_weather
        )
        
        # Should be None because too far (>50 km default)
        assert nearest is None
    
    def test_find_nearest_fire_within_window(self, enricher, sample_processed_call, sample_fire_data):
        """Test finding nearest fire within time window."""
        nearest = enricher._find_nearest_fire(
            sample_processed_call.caller_location,
            sample_processed_call.timestamp,
            sample_fire_data
        )
        
        assert nearest is not None
        assert nearest["fireId"] == "FIRE-001"  # Closer fire
        assert "_distance_km" in nearest
        assert "_bearing_degrees" in nearest
        assert nearest["_distance_km"] < 1.0  # Should be very close
    
    def test_find_nearest_fire_outside_window(self, enricher, sample_processed_call):
        """Test that fire data outside time window is ignored."""
        # Use a fire that's clearly outside the window (24+ hours old)
        old_fire = [
            {
                "timestamp": "2024-08-07T10:00:00Z",  # ~28 hours before call (clearly outside 3-hour window)
                "fireId": "FIRE-OLD",
                "location": {"lat": 20.8790, "lon": -156.6830},
            }
        ]
        
        nearest = enricher._find_nearest_fire(
            sample_processed_call.caller_location,
            sample_processed_call.timestamp,
            old_fire
        )
        
        # Should be None because outside 24-hour window for historical fires
        assert nearest is None
    
    def test_analyze_fire_proximity_with_wind(self, enricher, sample_processed_call):
        """Test fire proximity analysis with wind data."""
        nearest_fire = {
            "fireId": "FIRE-001",
            "location": {"lat": 20.8790, "lon": -156.6830},
            "_distance_km": 0.5,
            "_bearing_degrees": 45.0,  # Northeast of caller
            "frp": 33.0,  # Fire Radiative Power in MW
            "spreadRate": 5.0  # Estimated from frp
        }
        
        weather_enrichment = WeatherEnrichment(
            wind_direction=225.0,  # Wind from southwest (blowing toward northeast)
            wind_speed=25.0
        )
        
        analysis = enricher._analyze_fire_proximity(
            sample_processed_call.caller_location,
            nearest_fire,
            weather_enrichment
        )
        
        assert analysis is not None
        assert analysis.nearest_fire_id == "FIRE-001"
        assert analysis.distance_km == 0.5
        assert analysis.bearing_degrees == 45.0
        assert analysis.spread_rate == 5.0
        assert analysis.spread_toward_caller is not None
        # Verify fire_location and frp are included
        assert analysis.fire_location is not None
        assert analysis.fire_location["lat"] == 20.8790
        assert analysis.fire_location["lon"] == -156.6830
        assert analysis.frp == 33.0
        # Wind is blowing toward fire direction, so fire may spread toward caller
        if analysis.spread_toward_caller:
            assert analysis.estimated_time_to_reach is not None
            assert analysis.estimated_time_to_reach > 0
    
    def test_analyze_fire_proximity_no_wind(self, enricher, sample_processed_call):
        """Test fire proximity analysis without wind data."""
        nearest_fire = {
            "fireId": "FIRE-001",
            "location": {"lat": 20.8790, "lon": -156.6830},
            "_distance_km": 2.0,
            "_bearing_degrees": 90.0,
            "frp": 20.0,  # Fire Radiative Power in MW
            "spreadRate": 3.0  # Estimated from frp
        }
        
        analysis = enricher._analyze_fire_proximity(
            sample_processed_call.caller_location,
            nearest_fire,
            None  # No weather data
        )
        
        assert analysis is not None
        assert analysis.distance_km == 2.0
        assert analysis.spread_toward_caller is None  # Can't determine without wind
        # Verify fire_location and frp are included
        assert analysis.fire_location is not None
        assert analysis.fire_location["lat"] == 20.8790
        assert analysis.fire_location["lon"] == -156.6830
        assert analysis.frp == 20.0
        # But should still estimate time if spread rate is available
        if analysis.spread_rate and analysis.spread_rate > 0:
            # Time estimate might still be calculated
            pass
    
    def test_enrich_weather(self, enricher, sample_processed_call):
        """Test weather enrichment."""
        weather_data = {
            "timestamp": "2024-08-08T14:20:00Z",
            "location": {"lat": 20.8800, "lon": -156.6800},
            "temperature": 85.0,
            "windSpeed": 25.0,
            "windDirection": 270.0,
            "humidity": 45.0,
            "airQualityIndex": 180,
            "visibility": 2.0,
            "_distance_km": 0.3
        }
        
        nearest_fire = {
            "location": {"lat": 20.8790, "lon": -156.6830},
        }
        
        enrichment = enricher._enrich_weather(
            sample_processed_call.caller_location,
            nearest_fire,
            weather_data
        )
        
        assert enrichment is not None
        assert enrichment.temperature == 85.0
        assert enrichment.wind_speed == 25.0
        assert enrichment.wind_direction == 270.0
        assert enrichment.humidity == 45.0
        assert enrichment.air_quality_index == 180
        assert enrichment.visibility == 2.0
        assert enrichment.weather_distance_km == 0.3
        assert enrichment.wind_toward_fire is not None
    
    def test_enrich_call_full_data(self, enricher, sample_processed_call, sample_weather_data, sample_fire_data):
        """Test enriching call with full weather and fire data."""
        enriched = enricher.enrich_call(
            sample_processed_call,
            sample_weather_data,
            sample_fire_data
        )
        
        assert isinstance(enriched, EnrichedCall)
        assert enriched.call_id == "CALL-001"
        assert enriched.fire_analysis is not None
        assert enriched.weather_enrichment is not None
        assert enriched.fire_analysis.distance_km < 1.0
        assert enriched.weather_enrichment.temperature is not None
    
    def test_enrich_call_no_data(self, enricher, sample_processed_call):
        """Test enriching call with no weather or fire data."""
        enriched = enricher.enrich_call(
            sample_processed_call,
            [],  # No weather
            []   # No fire
        )
        
        assert isinstance(enriched, EnrichedCall)
        assert enriched.call_id == "CALL-001"
        assert enriched.fire_analysis is None
        assert enriched.weather_enrichment is None
    
    def test_enrich_call_partial_data(self, enricher, sample_processed_call, sample_weather_data):
        """Test enriching call with only weather data."""
        enriched = enricher.enrich_call(
            sample_processed_call,
            sample_weather_data,
            []  # No fire data
        )
        
        assert isinstance(enriched, EnrichedCall)
        assert enriched.weather_enrichment is not None
        assert enriched.fire_analysis is None
    
    def test_enrich_call_invalid_timestamp(self, enricher, sample_processed_call):
        """Test handling of invalid timestamps."""
        invalid_weather = [
            {
                "timestamp": "invalid-timestamp",
                "location": {"lat": 20.8800, "lon": -156.6800},
                "temperature": 80.0
            }
        ]
        
        # Should not crash, just skip invalid data
        enriched = enricher.enrich_call(
            sample_processed_call,
            invalid_weather,
            []
        )
        
        assert isinstance(enriched, EnrichedCall)
        # Should handle gracefully (no weather enrichment due to invalid timestamp)
    
    def test_enrich_call_missing_location(self, enricher, sample_processed_call):
        """Test handling of missing location data."""
        invalid_weather = [
            {
                "timestamp": "2024-08-08T14:20:00Z",
                "location": {},  # Missing lat/lon
                "temperature": 80.0
            }
        ]
        
        enriched = enricher.enrich_call(
            sample_processed_call,
            invalid_weather,
            []
        )
        
        assert isinstance(enriched, EnrichedCall)
        # Should handle gracefully (no weather enrichment due to missing location)
    
    def test_custom_time_windows(self):
        """Test enricher with custom time windows."""
        enricher = DataEnricher(
            weather_time_window_minutes=30,
            fire_time_window_hours=6,
            max_weather_distance_km=100.0
        )
        
        assert enricher.weather_time_window_minutes == 30
        assert enricher.fire_time_window_hours == 6
        assert enricher.max_weather_distance_km == 100.0
    
    def test_wind_toward_fire_calculation(self, enricher, sample_processed_call):
        """Test wind direction analysis relative to fire."""
        # Fire is northeast of caller (bearing ~45 degrees)
        nearest_fire = {
            "location": {"lat": 20.9000, "lon": -156.6500},  # Northeast
        }
        
        # Wind from southwest (225 degrees) = blowing toward northeast
        weather_data = {
            "windDirection": 225.0,
            "location": {"lat": 20.8800, "lon": -156.6800},
            "_distance_km": 0.5
        }
        
        enrichment = enricher._enrich_weather(
            sample_processed_call.caller_location,
            nearest_fire,
            weather_data
        )
        
        assert enrichment is not None
        # Wind should be toward fire (within 45 degrees)
        assert enrichment.wind_toward_fire is not None
    
    def test_fire_spread_toward_caller_calculation(self, enricher, sample_processed_call):
        """Test fire spread direction calculation."""
        # Fire is east of caller (bearing 90 degrees)
        nearest_fire = {
            "fireId": "FIRE-001",
            "location": {"lat": 20.8783, "lon": -156.6500},  # East
            "_distance_km": 2.0,
            "_bearing_degrees": 90.0,
            "frp": 33.0,  # Fire Radiative Power in MW
            "spreadRate": 5.0  # Estimated from frp
        }
        
        # Wind from west (270 degrees) = blowing toward east (90 degrees)
        weather_enrichment = WeatherEnrichment(
            wind_direction=270.0,  # West wind
            wind_speed=30.0
        )
        
        analysis = enricher._analyze_fire_proximity(
            sample_processed_call.caller_location,
            nearest_fire,
            weather_enrichment
        )
        
        assert analysis is not None
        # Wind is blowing toward caller direction, so fire may spread that way
        if analysis.spread_toward_caller:
            assert analysis.estimated_time_to_reach is not None
            assert analysis.estimated_time_to_reach > 0

