"""
Tests for Call Classification API endpoint.
"""

from fastapi.testclient import TestClient
from unittest.mock import patch

from sentinel_dispatch.api.main import app
from sentinel_dispatch.ai_ml.models import CallClassificationResult

client = TestClient(app)


class TestClassifyCallEndpoint:
    """Test cases for /api/classify-call endpoint."""
    
    def test_classify_call_success(self):
        """Test successful call classification."""
        mock_classification = CallClassificationResult(
            urgency=5,
            incident_type="fire",
            entities={
                "locations": [{"name": "Front Street", "lat": 20.8783, "lon": -156.6825}],
                "hazards": [{"hazard": "fire", "proximity": "close"}],
            },
            sentiment=0.9,
            key_phrases=["fire is close", "can't breathe"],
            people_affected=2,
            confidence=0.95
        )
        
        with patch("sentinel_dispatch.api.routers.classify.classify_call", return_value=mock_classification):
            response = client.post(
                "/api/classify-call",
                json={
                    "transcript": "I need help! The fire is getting close to my house on Front Street. I can't breathe!",
                    "call_id": "CALL-001"
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["call_id"] == "CALL-001"
        assert data["classification"]["urgency"] == 5
        assert data["classification"]["incident_type"] == "fire"
        assert data["classification"]["sentiment"] == 0.9
        assert "timestamp" in data
    
    def test_classify_call_without_call_id(self):
        """Test classification without providing call_id (should auto-generate)."""
        mock_classification = CallClassificationResult(
            urgency=3,
            incident_type="medical",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.8
        )
        
        with patch("sentinel_dispatch.api.routers.classify.classify_call", return_value=mock_classification):
            response = client.post(
                "/api/classify-call",
                json={
                    "transcript": "I need medical assistance."
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert "call_id" in data
        assert data["classification"]["urgency"] == 3
    
    def test_classify_call_with_metadata(self):
        """Test classification with metadata."""
        mock_classification = CallClassificationResult(
            urgency=4,
            incident_type="evacuation",
            entities={},
            sentiment=0.7,
            key_phrases=[],
            confidence=0.85
        )
        
        with patch("sentinel_dispatch.api.routers.classify.classify_call", return_value=mock_classification):
            response = client.post(
                "/api/classify-call",
                json={
                    "transcript": "We need to evacuate immediately!",
                    "call_id": "CALL-002",
                    "metadata": {
                        "phoneNumber": "+1-808-555-0123",
                        "callDuration": 120
                    }
                }
            )
        
        assert response.status_code == 200
        data = response.json()
        assert data["call_id"] == "CALL-002"
    
    def test_classify_call_empty_transcript(self):
        """Test classification with empty transcript (should fail validation)."""
        response = client.post(
            "/api/classify-call",
            json={
                "transcript": ""
            }
        )
        
        # FastAPI should validate and reject empty string
        assert response.status_code in [400, 422]
    
    def test_classify_call_missing_transcript(self):
        """Test classification without transcript (should fail validation)."""
        response = client.post(
            "/api/classify-call",
            json={}
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_classify_call_api_error(self):
        """Test handling of API errors during classification."""
        with patch("sentinel_dispatch.api.routers.classify.classify_call", side_effect=ValueError("API key missing")):
            response = client.post(
                "/api/classify-call",
                json={
                    "transcript": "Test transcript"
                }
            )
        
        assert response.status_code == 500
        data = response.json()
        assert "error" in data or "detail" in data
        assert "API key" in data.get("detail", "")
    
    def test_classify_call_internal_error(self):
        """Test handling of internal errors."""
        with patch("sentinel_dispatch.api.routers.classify.classify_call", side_effect=Exception("Unexpected error")):
            response = client.post(
                "/api/classify-call",
                json={
                    "transcript": "Test transcript"
                }
            )
        
        assert response.status_code == 500
        data = response.json()
        assert "error" in data or "detail" in data
    
    def test_classify_call_health_check(self):
        """Test health check endpoint."""
        response = client.get("/api/classify-call/health")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "call-classification"

