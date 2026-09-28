"""
Tests for call classification module.
"""

import json
import pytest
from unittest.mock import Mock, patch
from sentinel_dispatch.ai_ml.call_classifier import CallClassifier, classify_call
from sentinel_dispatch.ai_ml.models import CallClassificationResult


class TestCallClassifier:
    """Test cases for CallClassifier."""
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classifier_initialization_with_env_var(self, mock_client_class, monkeypatch):
        """Test classifier initialization with environment variable."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier()
        assert classifier.api_key == "test-key"
        assert classifier.model_name == "gemini-2.5-flash"
        mock_client_class.assert_called_once_with(api_key="test-key")
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classifier_initialization_with_api_key(self, mock_client_class):
        """Test classifier initialization with explicit API key."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier(api_key="explicit-key")
        assert classifier.api_key == "explicit-key"
        assert classifier.model_name == "gemini-2.5-flash"
        mock_client_class.assert_called_once_with(api_key="explicit-key")
    
    def test_classifier_initialization_missing_key(self, monkeypatch):
        """Test classifier initialization fails without API key."""
        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        with pytest.raises(ValueError, match="Gemini API key is required"):
            CallClassifier()
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classify_call_success(self, mock_client_class):
        """Test successful call classification with structured entities."""
        # Mock Gemini API response with structured entities
        mock_response = Mock()
        mock_response.text = json.dumps({
            "urgency": 5,
            "incident_type": "fire",
            "entities": {
                "locations": [
                    {
                        "name": "Front Street",
                        "lat": None,
                        "lon": None,
                        "context": "at"
                    },
                    {
                        "name": "my house",
                        "lat": None,
                        "lon": None,
                        "context": "near"
                    }
                ],
                "symptoms": [
                    {
                        "symptom": "difficulty breathing",
                        "severity": "severe"
                    },
                    {
                        "symptom": "smoke inhalation",
                        "severity": "moderate"
                    }
                ],
                "hazards": [
                    {
                        "hazard": "fire",
                        "proximity": "close"
                    },
                    {
                        "hazard": "smoke",
                        "proximity": "immediate"
                    }
                ],
                "people_mentioned": [
                    {
                        "role": "caller",
                        "status": "in danger",
                        "location": "Front Street"
                    },
                    {
                        "role": "neighbor",
                        "status": "trapped",
                        "location": "nearby house"
                    }
                ]
            },
            "sentiment": 0.9,
            "key_phrases": ["fire is close", "can't breathe", "need help now"],
            "people_affected": 2,
            "confidence": 0.95
        })
        
        mock_models = Mock()
        mock_models.generate_content.return_value = mock_response
        
        mock_client = Mock()
        mock_client.models = mock_models
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier(api_key="test-key")
        result = classifier.classify("I need help! The fire is getting close...")
        
        # Basic assertions
        assert isinstance(result, CallClassificationResult)
        assert result.urgency == 5
        assert result.incident_type == "fire"
        assert result.sentiment == 0.9
        assert result.confidence == 0.95
        assert result.people_affected == 2
        
        # Detailed entity assertions - Locations
        assert len(result.entities["locations"]) == 2
        location1 = result.entities["locations"][0]
        assert location1["name"] == "Front Street"
        assert location1["context"] == "at"
        assert location1.get("lat") is None  # Not mentioned in call
        
        location2 = result.entities["locations"][1]
        assert location2["name"] == "my house"
        assert location2["context"] == "near"
        
        # Detailed entity assertions - Symptoms
        assert len(result.entities["symptoms"]) == 2
        symptom1 = result.entities["symptoms"][0]
        assert symptom1["symptom"] == "difficulty breathing"
        assert symptom1["severity"] == "severe"
        
        symptom2 = result.entities["symptoms"][1]
        assert symptom2["symptom"] == "smoke inhalation"
        assert symptom2["severity"] == "moderate"
        
        # Detailed entity assertions - Hazards
        assert len(result.entities["hazards"]) == 2
        hazard1 = result.entities["hazards"][0]
        assert hazard1["hazard"] == "fire"
        assert hazard1["proximity"] == "close"
        
        hazard2 = result.entities["hazards"][1]
        assert hazard2["hazard"] == "smoke"
        assert hazard2["proximity"] == "immediate"
        
        # Detailed entity assertions - People mentioned
        assert len(result.entities["people_mentioned"]) == 2
        person1 = result.entities["people_mentioned"][0]
        assert person1["role"] == "caller"
        assert person1["status"] == "in danger"
        assert person1["location"] == "Front Street"
        
        person2 = result.entities["people_mentioned"][1]
        assert person2["role"] == "neighbor"
        assert person2["status"] == "trapped"
        assert person2["location"] == "nearby house"
        
        # Key phrases assertions
        assert len(result.key_phrases) == 3
        assert "fire is close" in result.key_phrases
        assert "can't breathe" in result.key_phrases
        assert "need help now" in result.key_phrases
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classify_call_with_markdown(self, mock_client_class):
        """Test classification handles markdown code blocks in response."""
        mock_response = Mock()
        mock_response.text = "```json\n" + json.dumps({
            "urgency": 4,
            "incident_type": "medical",
            "entities": {
                "locations": [
                    {"name": "Lahainaluna Road", "lat": None, "lon": None, "context": "at"}
                ],
                "symptoms": [
                    {"symptom": "asthma attack", "severity": "critical"}
                ],
                "hazards": [],
                "people_mentioned": [
                    {"role": "caller", "status": "in danger", "location": None}
                ]
            },
            "sentiment": 0.7,
            "key_phrases": ["can't breathe", "asthma"],
            "people_affected": 1,
            "confidence": 0.85
        }) + "\n```"
        
        mock_models = Mock()
        mock_models.generate_content.return_value = mock_response
        
        mock_client = Mock()
        mock_client.models = mock_models
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier(api_key="test-key")
        result = classifier.classify("Test transcript")
        
        # Basic assertions
        assert result.urgency == 4
        assert result.incident_type == "medical"
        assert result.sentiment == 0.7
        assert result.confidence == 0.85
        assert result.people_affected == 1
        
        # Entity structure assertions
        assert len(result.entities["locations"]) == 1
        assert result.entities["locations"][0]["name"] == "Lahainaluna Road"
        assert result.entities["locations"][0]["context"] == "at"
        
        assert len(result.entities["symptoms"]) == 1
        assert result.entities["symptoms"][0]["symptom"] == "asthma attack"
        assert result.entities["symptoms"][0]["severity"] == "critical"
        
        assert len(result.entities["hazards"]) == 0
        
        assert len(result.entities["people_mentioned"]) == 1
        assert result.entities["people_mentioned"][0]["role"] == "caller"
        assert result.entities["people_mentioned"][0]["status"] == "in danger"
        
        assert len(result.key_phrases) == 2
        assert "can't breathe" in result.key_phrases
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classify_call_invalid_json(self, mock_client_class):
        """Test classification handles invalid JSON response."""
        mock_response = Mock()
        mock_response.text = "This is not JSON"
        
        mock_models = Mock()
        mock_models.generate_content.return_value = mock_response
        
        mock_client = Mock()
        mock_client.models = mock_models
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier(api_key="test-key")
        
        with pytest.raises(ValueError, match="Failed to parse Gemini response"):
            classifier.classify("Test transcript")
    
    def test_classify_convenience_function(self, monkeypatch):
        """Test convenience function works."""
        monkeypatch.setenv("GEMINI_API_KEY", "test-key")
        
        with patch('sentinel_dispatch.ai_ml.call_classifier.CallClassifier') as mock_classifier_class:
            mock_classifier = Mock()
            mock_result = CallClassificationResult(
                urgency=3,
                incident_type="other",
                entities={
                    "locations": [
                        {"name": "Kihei", "lat": None, "lon": None, "context": None}
                    ],
                    "symptoms": [],
                    "hazards": [
                        {"hazard": "smoke", "proximity": "distant"}
                    ],
                    "people_mentioned": []
                },
                sentiment=0.5,
                key_phrases=["monitoring"],
                people_affected=None,
                confidence=0.8
            )
            mock_classifier.classify.return_value = mock_result
            mock_classifier_class.return_value = mock_classifier
            
            result = classify_call("Test transcript")
            
            # Basic assertions
            assert result.urgency == 3
            assert result.incident_type == "other"
            
            # Entity structure assertions
            assert len(result.entities["locations"]) == 1
            assert result.entities["locations"][0]["name"] == "Kihei"
            
            assert len(result.entities["hazards"]) == 1
            assert result.entities["hazards"][0]["hazard"] == "smoke"
            assert result.entities["hazards"][0]["proximity"] == "distant"
            
            mock_classifier.classify.assert_called_once_with("Test transcript")
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classify_call_with_empty_entities(self, mock_client_class):
        """Test classification with minimal entities (empty lists)."""
        mock_response = Mock()
        mock_response.text = json.dumps({
            "urgency": 2,
            "incident_type": "other",
            "entities": {
                "locations": [],
                "symptoms": [],
                "hazards": [],
                "people_mentioned": []
            },
            "sentiment": 0.2,
            "key_phrases": [],
            "people_affected": None,
            "confidence": 0.75
        })
        
        mock_models = Mock()
        mock_models.generate_content.return_value = mock_response
        
        mock_client = Mock()
        mock_client.models = mock_models
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier(api_key="test-key")
        result = classifier.classify("Just checking in...")
        
        # Verify all entity lists are empty but present
        assert result.entities["locations"] == []
        assert result.entities["symptoms"] == []
        assert result.entities["hazards"] == []
        assert result.entities["people_mentioned"] == []
        assert result.key_phrases == []
        assert result.people_affected is None
    
    @patch('sentinel_dispatch.ai_ml.call_classifier.genai.Client')
    def test_classify_call_with_optional_fields(self, mock_client_class):
        """Test classification handles optional entity fields correctly."""
        mock_response = Mock()
        mock_response.text = json.dumps({
            "urgency": 4,
            "incident_type": "evacuation",
            "entities": {
                "locations": [
                    {
                        "name": "Kaanapali Beach",
                        "lat": 20.9289,
                        "lon": -156.6944,
                        "context": "at"
                    }
                ],
                "symptoms": [
                    {
                        "symptom": "minor smoke irritation",
                        "severity": None  # Optional field
                    }
                ],
                "hazards": [
                    {
                        "hazard": "fire",
                        "proximity": None  # Optional field
                    }
                ],
                "people_mentioned": [
                    {
                        "role": "caller",
                        "status": None,  # Optional field
                        "location": None  # Optional field
                    }
                ]
            },
            "sentiment": 0.6,
            "key_phrases": ["need to evacuate"],
            "people_affected": 10,
            "confidence": 0.88
        })
        
        mock_models = Mock()
        mock_models.generate_content.return_value = mock_response
        
        mock_client = Mock()
        mock_client.models = mock_models
        mock_client_class.return_value = mock_client
        
        classifier = CallClassifier(api_key="test-key")
        result = classifier.classify("We need to evacuate...")
        
        # Verify location with coordinates
        location = result.entities["locations"][0]
        assert location["name"] == "Kaanapali Beach"
        assert location["lat"] == 20.9289
        assert location["lon"] == -156.6944
        assert location["context"] == "at"
        
        # Verify optional fields can be null
        symptom = result.entities["symptoms"][0]
        assert symptom["symptom"] == "minor smoke irritation"
        assert symptom.get("severity") is None
        
        hazard = result.entities["hazards"][0]
        assert hazard["hazard"] == "fire"
        assert hazard.get("proximity") is None
        
        person = result.entities["people_mentioned"][0]
        assert person["role"] == "caller"
        assert person.get("status") is None
        assert person.get("location") is None
        
        assert result.people_affected == 10


if __name__ == "__main__":
    pytest.main([__file__])

