"""
Tests for Data Enrichment API endpoint.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from sentinel_dispatch.api.main import app
from sentinel_dispatch.ai_ml.models import (
    CallClassificationResult,
    EnrichedCall,
    FireProximityAnalysis,
    WeatherEnrichment,
)

client = TestClient(app)


class TestEnrichCallEndpoint:
    """Test cases for /api/enrich-call endpoint."""
    
    @pytest.fixture
    def sample_processed_call(self):
        """Create a sample processed call."""
        return {
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:45Z",
            "caller_location": {"lat": 20.8783, "lon": -156.6825},
            "transcript": "Fire is getting close!",
            "classification": {
                "urgency": 5,
                "incident_type": "fire",
                "entities": {},
                "sentiment": 0.9,
                "key_phrases": [],
                "confidence": 0.95
            }
        }
    
    def test_enrich_call_success(self, sample_processed_call):
        """Test successful call enrichment."""
        mock_enriched = EnrichedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Fire is getting close!",
            classification=CallClassificationResult(
                urgency=5,
                incident_type="fire",
                entities={},
                sentiment=0.9,
                key_phrases=[],
                confidence=0.95
            ),
            fire_analysis=FireProximityAnalysis(
                nearest_fire_id="FIRE-001",
                distance_km=0.5,
                bearing_degrees=45.0,
                spread_rate=12.0,
                spread_toward_caller=True,
                estimated_time_to_reach=0.04,
                fire_location={"lat": 20.8790, "lon": -156.6830},
                frp=33.0,  # Fire Radiative Power in MW
            ),
            weather_enrichment=WeatherEnrichment(
                temperature=85.0,
                wind_speed=25.0,
                wind_direction=270.0,
                humidity=45.0,
            ),
            enrichment_timestamp="2024-08-08T14:23:50Z",
        )
        
        with patch("sentinel_dispatch.api.routers.enrich.enrich_call", return_value=mock_enriched):
            response = client.post(
                "/api/enrich-call",
                json={
                    "processed_call": sample_processed_call,
                    "weather_data_list": [
                        {
                            "timestamp": "2024-08-08T14:20:00Z",
                            "location": {"lat": 20.8800, "lon": -156.6800},
                            "temperature": 85.0,
                            "windSpeed": 25.0,
                        }
                    ],
                    "fire_data_list": [
                        {
                            "timestamp": "2024-08-08T14:00:00Z",
                            "fireId": "FIRE-001",
                            "location": {"lat": 20.8790, "lon": -156.6830},
                            "frp": 80.0,  # Fire Radiative Power in MW
                            "spreadRate": 12.0,  # Estimated from frp
                        }
                    ]
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["enriched_call"]["call_id"] == "CALL-001"
        assert data["enriched_call"]["fire_analysis"] is not None
        assert data["enriched_call"]["fire_analysis"]["distance_km"] == 0.5
        assert data["enriched_call"]["weather_enrichment"] is not None
        assert "timestamp" in data
    
    def test_enrich_call_no_data(self, sample_processed_call):
        """Test enrichment with no weather or fire data."""
        mock_enriched = EnrichedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Fire is getting close!",
            classification=CallClassificationResult(
                urgency=5,
                incident_type="fire",
                entities={},
                sentiment=0.9,
                key_phrases=[],
                confidence=0.95
            ),
            fire_analysis=None,
            weather_enrichment=None,
            enrichment_timestamp="2024-08-08T14:23:50Z",
        )
        
        with patch("sentinel_dispatch.api.routers.enrich.enrich_call", return_value=mock_enriched):
            response = client.post(
                "/api/enrich-call",
                json={
                    "processed_call": sample_processed_call,
                    "weather_data_list": [],
                    "fire_data_list": []
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["enriched_call"]["fire_analysis"] is None
        assert data["enriched_call"]["weather_enrichment"] is None
    
    def test_enrich_call_missing_processed_call(self):
        """Test enrichment without processed_call (should fail validation)."""
        response = client.post(
            "/api/enrich-call",
            json={
                "weather_data_list": []
            }
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_enrich_call_validation_error(self, sample_processed_call):
        """Test handling of validation errors."""
        with patch("sentinel_dispatch.api.routers.enrich.enrich_call", side_effect=ValueError("Invalid call data")):
            response = client.post(
                "/api/enrich-call",
                json={
                    "processed_call": sample_processed_call
                }
            )
        
        assert response.status_code == 400
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_enrich_call_internal_error(self, sample_processed_call):
        """Test handling of internal errors."""
        with patch("sentinel_dispatch.api.routers.enrich.enrich_call", side_effect=Exception("Unexpected error")):
            response = client.post(
                "/api/enrich-call",
                json={
                    "processed_call": sample_processed_call
                }
            )
        
        assert response.status_code == 500
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_enrich_call_health_check(self):
        """Test health check endpoint."""
        response = client.get("/api/enrich-call/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "data-enrichment"

