"""
Tests for Alert Generation API endpoint.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from sentinel_dispatch.api.main import app
from sentinel_dispatch.ai_ml.alert_generator import Alert, AlertSeverity, AlertType

client = TestClient(app)


class TestGenerateAlertEndpoint:
    """Test cases for /api/generate-alert endpoint."""
    
    @pytest.fixture
    def sample_risk_score(self):
        """Create a sample risk score."""
        return {
            "call_id": "CALL-001",
            "timestamp": "2024-08-08T14:23:50Z",
            "risk_score": 85.0,
            "confidence": 0.95,
            "factors": {
                "call_urgency": 0.9,
                "fire_proximity": 0.8,
                "fire_spread_rate": 0.7,
                "weather_risk": 0.6,
                "vulnerability": 0.8,
                "time_since_call": 0.3,
            },
            "recommendation": "IMMEDIATE_DISPATCH",
            "priority": 1,
        }
    
    def test_generate_alert_success(self, sample_risk_score):
        """Test successful alert generation."""
        mock_alerts = [
            Alert(
                alert_id="alert-1",
                timestamp="2024-08-08T14:23:50Z",
                severity=AlertSeverity.CRITICAL,
                message="High risk score detected: 85.0/100. Recommendation: IMMEDIATE_DISPATCH",
                alert_type=AlertType.RISK_THRESHOLD,
                call_id="CALL-001",
                headline="Risk Score Alert: 85.0/100",
                description="Emergency call CALL-001 has a risk score of 85.0/100",
                urgency="immediate",
                certainty="observed",
                effective="2024-08-08T14:23:50Z",
                expires="2024-08-08T15:23:50Z",
                status="active",
            )
        ]
        
        with patch("sentinel_dispatch.api.routers.alerts.generate_alerts", return_value=mock_alerts):
            response = client.post(
                "/api/generate-alert",
                json={
                    "risk_score": sample_risk_score
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["alert_count"] == 1
        assert len(data["alerts"]) == 1
        assert data["alerts"][0]["severity"] == "critical"
        assert data["alerts"][0]["alert_type"] == "risk_threshold"
        assert "timestamp" in data
    
    def test_generate_alert_with_enriched_call(self, sample_risk_score):
        """Test alert generation with enriched call data."""
        mock_alerts = [
            Alert(
                alert_id="alert-1",
                timestamp="2024-08-08T14:23:50Z",
                severity=AlertSeverity.CRITICAL,
                message="High risk score detected",
                alert_type=AlertType.RISK_THRESHOLD,
                call_id="CALL-001",
            ),
            Alert(
                alert_id="alert-2",
                timestamp="2024-08-08T14:23:50Z",
                severity=AlertSeverity.CRITICAL,
                message="Fire is extremely close",
                alert_type=AlertType.FIRE_PROXIMITY,
                call_id="CALL-001",
            ),
        ]
        
        enriched_call = {
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
            },
            "fire_analysis": {
                "nearest_fire_id": "FIRE-001",
                "distance_km": 0.5,
                "bearing_degrees": 45.0,
            },
            "enrichment_timestamp": "2024-08-08T14:23:50Z",
        }
        
        with patch("sentinel_dispatch.api.routers.alerts.generate_alerts", return_value=mock_alerts):
            response = client.post(
                "/api/generate-alert",
                json={
                    "risk_score": sample_risk_score,
                    "enriched_call": enriched_call
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["alert_count"] == 2
        assert len(data["alerts"]) == 2
    
    def test_generate_alert_low_priority(self):
        """Test alert generation for low risk scores (now generates low priority alerts)."""
        low_risk_score = {
            "call_id": "CALL-002",
            "timestamp": "2024-08-08T14:23:50Z",
            "risk_score": 15.0,  # Low risk
            "confidence": 0.8,
            "factors": {
                "call_urgency": 0.2,
                "fire_proximity": 0.1,
                "vulnerability": 0.2,
            },
            "recommendation": "MONITOR",
            "priority": 5,
        }
        
        mock_alerts = [
            Alert(
                alert_id="alert-low-1",
                timestamp="2024-08-08T14:23:50Z",
                severity=AlertSeverity.LOW,
                message="High risk score detected: 15.0/100. Recommendation: MONITOR",
                alert_type=AlertType.RISK_THRESHOLD,
                call_id="CALL-002",
                headline="Risk Score Alert: 15.0/100",
                description="Emergency call CALL-002 has a risk score of 15.0/100",
                urgency="future",
                certainty="possible",
                effective="2024-08-08T14:23:50Z",
                expires="2024-08-08T22:23:50Z",
                status="active",
            )
        ]
        
        with patch("sentinel_dispatch.api.routers.alerts.generate_alerts", return_value=mock_alerts):
            response = client.post(
                "/api/generate-alert",
                json={
                    "risk_score": low_risk_score
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["alert_count"] == 1
        assert len(data["alerts"]) == 1
        assert data["alerts"][0]["severity"] == "low"
        assert data["alerts"][0]["alert_type"] == "risk_threshold"
    
    def test_generate_alert_missing_risk_score(self):
        """Test alert generation without risk_score (should fail validation)."""
        response = client.post(
            "/api/generate-alert",
            json={}
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_generate_alert_validation_error(self, sample_risk_score):
        """Test handling of validation errors."""
        with patch("sentinel_dispatch.api.routers.alerts.generate_alerts", side_effect=ValueError("Invalid risk score")):
            response = client.post(
                "/api/generate-alert",
                json={
                    "risk_score": sample_risk_score
                }
            )
        
        assert response.status_code == 400
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_generate_alert_internal_error(self, sample_risk_score):
        """Test handling of internal errors."""
        with patch("sentinel_dispatch.api.routers.alerts.generate_alerts", side_effect=Exception("Unexpected error")):
            response = client.post(
                "/api/generate-alert",
                json={
                    "risk_score": sample_risk_score
                }
            )
        
        assert response.status_code == 500
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_generate_alert_health_check(self):
        """Test health check endpoint."""
        response = client.get("/api/generate-alert/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "alert-generation"

