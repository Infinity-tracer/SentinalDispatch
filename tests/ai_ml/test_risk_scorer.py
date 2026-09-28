"""
Tests for risk scoring module.
"""
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch
from datetime import datetime, timezone, timedelta

import pytest

from sentinel_dispatch.ai_ml import VertexAIRiskScorer
from sentinel_dispatch.ai_ml.models import (
    CallClassificationResult,
    RiskScoreInput,
    RiskScoreResult,
    RiskFactors,
)

if TYPE_CHECKING:
    from sentinel_dispatch.ai_ml.risk_scorer import RiskScorer


class TestRiskScorer:
    """Test cases for RiskScorer."""

    @pytest.fixture
    def scorer(self):
        from sentinel_dispatch.ai_ml.risk_scorer import RiskScorer

        return RiskScorer()
    
    def test_scorer_initialization(self, scorer: "RiskScorer"):
        """Test risk scorer initialization (hybrid mode by default)."""
        assert scorer.weights["call_urgency"] == 0.30
        assert scorer.weights["fire_proximity"] == 0.25
        assert scorer.weights["vulnerability"] == 0.15
        # Should attempt to initialize Vertex AI but fall back to rule-based if unavailable
    
    def test_calculate_call_urgency_factor(self, scorer: "RiskScorer"):
        """Test call urgency factor calculation."""
        # Moderate urgency to allow phase boost to be visible (not already at 1.0)
        classification = CallClassificationResult(
            urgency=4,
            incident_type="fire",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.9
        )
        factor = scorer.calculate_call_urgency_factor(classification)
        assert 0.0 <= factor <= 1.0
        # Urgency 4 normalized: (4-1)/4 = 0.75, sentiment boost: 0.5 * 0.2 = 0.1
        # Total: 0.75 + 0.1 = 0.85
        assert 0.8 <= factor <= 0.9  # Should be high but not at cap
        
        # Moderate urgency with peak phase boost
        factor_peak = scorer.calculate_call_urgency_factor(classification, phase="peak")
        assert factor_peak > factor  # Peak phase should boost urgency (+0.15)
        assert factor_peak <= 1.0
        assert abs(factor_peak - (factor + 0.15)) < 0.01  # Should be exactly +0.15
        
        # Moderate urgency with critical phase boost
        factor_critical = scorer.calculate_call_urgency_factor(classification, phase="critical")
        assert factor_critical > factor  # Critical phase should boost urgency (+0.10)
        assert factor_critical < factor_peak  # Peak should be higher than critical
        assert abs(factor_critical - (factor + 0.10)) < 0.01  # Should be exactly +0.10
        
        # Moderate urgency with developing phase boost
        factor_developing = scorer.calculate_call_urgency_factor(classification, phase="developing")
        assert factor_developing > factor  # Developing phase should boost urgency (+0.05)
        assert factor_developing < factor_critical  # Critical should be higher than developing
        assert abs(factor_developing - (factor + 0.05)) < 0.01  # Should be exactly +0.05
        
        # Low urgency, low sentiment
        classification = CallClassificationResult(
            urgency=1,
            incident_type="other",
            entities={},
            sentiment=0.1,
            key_phrases=[],
            confidence=0.8
        )
        factor = scorer.calculate_call_urgency_factor(classification)
        assert 0.0 <= factor <= 1.0
        assert factor < 0.3  # Should be low
        
        # Low urgency with early phase (no boost)
        factor_early = scorer.calculate_call_urgency_factor(classification, phase="early")
        assert factor_early == factor  # Early phase should not boost
        
        # Low urgency with aftermath phase (no boost)
        factor_aftermath = scorer.calculate_call_urgency_factor(classification, phase="aftermath")
        assert factor_aftermath == factor  # Aftermath phase should not boost
    
    def test_calculate_fire_proximity_factor(self, scorer: "RiskScorer"):
        """Test fire proximity factor calculation."""
        caller_location = {"lat": 20.8783, "lon": -156.6825}
        
        # Very close fire (< 1km)
        nearest_fire = {
            "location": {"lat": 20.8790, "lon": -156.6830},
            "distance_km": 0.5
        }
        factor = scorer.calculate_fire_proximity_factor(caller_location, nearest_fire)
        assert factor == 1.0  # Maximum risk
        
        # Far fire (> 20km)
        nearest_fire = {
            "location": {"lat": 20.9000, "lon": -156.7000},
            "distance_km": 25.0
        }
        factor = scorer.calculate_fire_proximity_factor(caller_location, nearest_fire)
        assert 0.0 <= factor < 0.3  # Low risk
        
        # No fire
        factor = scorer.calculate_fire_proximity_factor(caller_location, None)
        assert factor == 0.0
    
    def test_calculate_fire_spread_rate_factor(self, scorer: "RiskScorer"):
        """Test fire spread rate factor calculation using FRP (Fire Radiative Power)."""
        caller_location = {"lat": 20.8783, "lon": -156.6825}
        
        # High intensity fire (FRP > 100 MW = very high intensity)
        nearest_fire = {"frp": 150.0}  # MW (Fire Radiative Power)
        factor = scorer.calculate_fire_spread_rate_factor(caller_location, nearest_fire)
        assert factor >= 0.9  # Very high intensity (0.9-1.0)
        
        # Moderate intensity fire (FRP 10-50 MW)
        nearest_fire = {"frp": 30.0}  # MW
        factor = scorer.calculate_fire_spread_rate_factor(caller_location, nearest_fire)
        assert 0.3 <= factor <= 0.7  # Moderate intensity
        
        # Low intensity fire (FRP < 10 MW)
        nearest_fire = {"frp": 5.0}  # MW
        factor = scorer.calculate_fire_spread_rate_factor(caller_location, nearest_fire)
        assert 0.0 < factor < 0.3  # Low intensity
        
        # Fallback to spread_rate if frp not available
        nearest_fire = {"spread_rate": 15.0}  # km/h
        factor = scorer.calculate_fire_spread_rate_factor(caller_location, nearest_fire)
        assert factor == 1.0  # Capped at 1.0
        
        # Slow spread (fallback)
        nearest_fire = {"spread_rate": 2.0}  # km/h
        factor = scorer.calculate_fire_spread_rate_factor(caller_location, nearest_fire)
        assert 0.0 < factor < 0.5
        
        # No fire data
        nearest_fire = {}
        factor = scorer.calculate_fire_spread_rate_factor(caller_location, nearest_fire)
        assert factor == 0.0
    
    def test_calculate_weather_risk_factor(self, scorer: "RiskScorer"):
        """Test weather risk factor calculation."""
        # High wind, poor air quality
        weather_data = {
            "wind_speed": 45.0,  # mph
            "air_quality_index": 180,
            "humidity": 25.0
        }
        factor = scorer.calculate_weather_risk_factor(weather_data)
        assert 0.0 <= factor <= 1.0
        assert factor > 0.7  # Should be high
        
        # Low wind, good air quality
        weather_data = {
            "wind_speed": 5.0,
            "air_quality_index": 30,
            "humidity": 80.0
        }
        factor = scorer.calculate_weather_risk_factor(weather_data)
        assert 0.0 <= factor < 0.3  # Should be low
        
        # No weather data
        factor = scorer.calculate_weather_risk_factor(None)
        assert factor == 0.0
    
    def test_calculate_vulnerability_factor(self, scorer: "RiskScorer"):
        """Test vulnerability factor calculation."""
        # High vulnerability: elderly with medical conditions
        vulnerability = {
            "age": 75,
            "medical_conditions": ["asthma", "heart_disease"],
            "mobility": "limited"
        }
        factor = scorer.calculate_vulnerability_factor(vulnerability)
        assert 0.0 <= factor <= 1.0
        assert factor > 0.7  # Should be high
        
        # Low vulnerability: healthy adult
        vulnerability = {
            "age": 35,
            "medical_conditions": [],
            "mobility": "normal"
        }
        factor = scorer.calculate_vulnerability_factor(vulnerability)
        assert 0.0 <= factor < 0.3  # Should be low
        
        # No vulnerability data (default)
        factor = scorer.calculate_vulnerability_factor(None)
        assert factor == 0.3  # Default moderate
    
    def test_calculate_time_since_call_factor(self, scorer: "RiskScorer"):
        """Test time since call factor calculation."""
        # Just called
        call_time = datetime.now(timezone.utc).isoformat()
        factor = scorer.calculate_time_since_call_factor(call_time, None)
        assert factor == 0.0
        
        # Called 10 minutes ago
        call_time = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        factor = scorer.calculate_time_since_call_factor(call_time, None)
        assert 0.0 < factor < 0.5
        
        # Called 45 minutes ago
        call_time = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
        factor = scorer.calculate_time_since_call_factor(call_time, None)
        assert 0.5 < factor < 1.0
        
        # Called 2 hours ago
        call_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        factor = scorer.calculate_time_since_call_factor(call_time, None)
        assert factor == 1.0
    
    def test_determine_recommendation(self):
        """Test recommendation determination."""
        from sentinel_dispatch.ai_ml.risk_utils import determine_recommendation
        
        assert determine_recommendation(90.0) == "IMMEDIATE_DISPATCH"
        assert determine_recommendation(70.0) == "HIGH_PRIORITY"
        assert determine_recommendation(50.0) == "MODERATE_PRIORITY"
        assert determine_recommendation(25.0) == "LOW_PRIORITY"
        assert determine_recommendation(10.0) == "MONITOR"
    
    def test_determine_priority(self):
        """Test priority determination."""
        from sentinel_dispatch.ai_ml.risk_utils import determine_priority
        
        # Priority 5 = Critical/Immediate (highest risk)
        # Priority 1 = Low priority (lowest risk)
        assert determine_priority(90.0) == 5  # High risk -> critical priority
        assert determine_priority(70.0) == 4  # High risk -> high priority
        assert determine_priority(50.0) == 3  # Medium risk -> moderate priority
        assert determine_priority(25.0) == 2  # Low-moderate risk -> low-moderate priority
        assert determine_priority(10.0) == 1  # Low risk -> low priority
    
    def test_calculate_risk_score_high_risk(self, scorer: "RiskScorer"):
        """Test risk score calculation for high-risk scenario."""
        classification = CallClassificationResult(
            urgency=5,
            incident_type="fire",
            entities={},
            sentiment=0.9,
            key_phrases=["fire is close", "can't breathe"],
            people_affected=2,
            confidence=0.95
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-001",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification,
            nearest_fire={
                "location": {"lat": 20.8790, "lon": -156.6830},
                "distance_km": 0.5,
                "frp": 80.0,  # Fire Radiative Power in MW (high intensity)
                "spread_rate": 12.0  # Fallback
            },
            weather_data={
                "wind_speed": 40.0,
                "air_quality_index": 200,
                "humidity": 20.0
            },
            vulnerability_factors={
                "age": 75,
                "medical_conditions": ["asthma"],
                "mobility": "limited"
            }
        )
        
        result = scorer.calculate_risk_score(input_data)
        
        assert result.call_id == "CALL-001"
        assert 0.0 <= result.risk_score <= 100.0
        assert result.risk_score > 70.0  # Should be high risk
        assert 0.0 <= result.confidence <= 1.0
        assert result.recommendation in ["IMMEDIATE_DISPATCH", "HIGH_PRIORITY"]
        assert result.priority in [4, 5]  # High risk -> priority 4 or 5 (not 1 or 2)
        assert result.factors.call_urgency > 0.8
        assert result.factors.fire_proximity > 0.8
        assert result.factors.vulnerability > 0.7
    
    def test_calculate_risk_score_low_risk(self, scorer: "RiskScorer"):
        """Test risk score calculation for low-risk scenario."""
        classification = CallClassificationResult(
            urgency=2,
            incident_type="other",
            entities={},
            sentiment=0.2,
            key_phrases=[],
            confidence=0.8
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-002",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification,
            nearest_fire={
                "location": {"lat": 20.9000, "lon": -156.7000},
                "distance_km": 25.0,
                "frp": 3.0,  # Fire Radiative Power in MW (low intensity)
                "spread_rate": 0.5  # Fallback
            },
            weather_data={
                "wind_speed": 5.0,
                "air_quality_index": 30,
                "humidity": 80.0
            },
            vulnerability_factors={
                "age": 35,
                "medical_conditions": [],
                "mobility": "normal"
            }
        )
        
        result = scorer.calculate_risk_score(input_data)
        
        assert result.call_id == "CALL-002"
        assert 0.0 <= result.risk_score <= 100.0
        assert result.risk_score < 40.0  # Should be low risk
        assert result.recommendation in ["LOW_PRIORITY", "MONITOR"]
        assert result.priority in [1, 2]  # Low risk -> priority 1 or 2 (not 4 or 5)
    
    def test_calculate_risk_score_missing_data(self, scorer: "RiskScorer"):
        """Test risk score calculation with missing optional data."""
        classification = CallClassificationResult(
            urgency=4,
            incident_type="fire",
            entities={},
            sentiment=0.7,
            key_phrases=["need help"],
            confidence=0.85
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-003",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification,
            nearest_fire=None,  # Missing fire data
            weather_data=None,  # Missing weather data
            vulnerability_factors=None  # Missing vulnerability data
        )
        
        result = scorer.calculate_risk_score(input_data)
        
        assert result.call_id == "CALL-003"
        assert 0.0 <= result.risk_score <= 100.0
        assert result.factors.fire_proximity == 0.0
        assert result.factors.weather_risk is None or result.factors.weather_risk == 0.0
        assert result.factors.vulnerability == 0.3  # Default value
        # Confidence should be lower due to missing data
        assert result.confidence < 0.9
    
    def test_calculate_risk_score_convenience_function(self):
        """Test convenience function."""
        from sentinel_dispatch.ai_ml.risk_scorer import calculate_risk_score

        classification = CallClassificationResult(
            urgency=3,
            incident_type="medical",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.8
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-004",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        result = calculate_risk_score(input_data)
        
        assert result.call_id == "CALL-004"
        assert 0.0 <= result.risk_score <= 100.0
        # Verify it's a RiskScoreResult by checking attributes
        assert hasattr(result, "risk_score")
        assert hasattr(result, "factors")
        assert hasattr(result, "recommendation")

    def test_haversine_distance(self):
        """Test Haversine distance calculation."""
        from sentinel_dispatch.utils.geospatial import haversine_distance

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
    
    def test_scorer_hybrid_mode_without_vertex_ai(self, scorer: "RiskScorer"):
        """Test hybrid mode (default) falls back to rule-based when Vertex AI unavailable."""
        classification = CallClassificationResult(
            urgency=4,
            incident_type="fire",
            entities={},
            sentiment=0.7,
            key_phrases=[],
            confidence=0.85
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-HYBRID",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        # Should use rule-based fallback
        result = scorer.calculate_risk_score(input_data)
        assert isinstance(result, RiskScoreResult)
        assert 0.0 <= result.risk_score <= 100.0

    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer')
    def test_scorer_hybrid_mode_with_vertex_ai(self, mock_vertex_ai_class):
        """Test hybrid mode (default) uses Vertex AI when available."""
        # Mock Vertex AI scorer
        mock_vertex_scorer = Mock(spec=VertexAIRiskScorer)
        mock_vertex_scorer.is_available.return_value = True
        mock_vertex_scorer.calculate_risk_score.return_value = RiskScoreResult(
            call_id="CALL-HYBRID",
            timestamp=datetime.now(timezone.utc).isoformat(),
            risk_score=85.0,
            confidence=0.95,
            factors=Mock(spec=RiskFactors),
            recommendation="IMMEDIATE_DISPATCH",
            priority=1
        )
        mock_vertex_ai_class.return_value = mock_vertex_scorer

        from sentinel_dispatch.ai_ml.risk_scorer import RiskScorer
        scorer = RiskScorer(mock_vertex_scorer)
        
        classification = CallClassificationResult(
            urgency=5,
            incident_type="fire",
            entities={},
            sentiment=0.9,
            key_phrases=[],
            confidence=0.95
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-HYBRID",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        # Should use Vertex AI
        result = scorer.calculate_risk_score(input_data)
        assert result.risk_score == 85.0
        mock_vertex_scorer.calculate_risk_score.assert_called_once_with(input_data)

    @patch('sentinel_dispatch.ai_ml.vertex_ai_scorer.VertexAIRiskScorer')
    def test_scorer_hybrid_mode_fallback_on_error(self, mock_vertex_ai_class):
        """Test hybrid mode (default) falls back to rule-based when Vertex AI fails."""
        # Mock Vertex AI scorer that raises error
        mock_vertex_scorer = Mock()
        mock_vertex_scorer.is_available.return_value = True
        mock_vertex_scorer.calculate_risk_score.side_effect = Exception("Vertex AI error")
        mock_vertex_ai_class.return_value = mock_vertex_scorer

        from sentinel_dispatch.ai_ml.risk_scorer import RiskScorer
        scorer = RiskScorer(mock_vertex_scorer)
        
        classification = CallClassificationResult(
            urgency=3,
            incident_type="medical",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.8
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-FALLBACK",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        # Should fall back to rule-based
        result = scorer.calculate_risk_score(input_data)
        assert isinstance(result, RiskScoreResult)
        assert 0.0 <= result.risk_score <= 100.0
    
    def test_calculate_risk_score_convenience_function_uses_hybrid(self):
        """Test convenience function uses hybrid mode by default."""
        from sentinel_dispatch.ai_ml.risk_scorer import calculate_risk_score

        classification = CallClassificationResult(
            urgency=3,
            incident_type="other",
            entities={},
            sentiment=0.5,
            key_phrases=[],
            confidence=0.8
        )
        
        input_data = RiskScoreInput(
            call_id="CALL-CONVENIENCE",
            call_timestamp=datetime.now(timezone.utc).isoformat(),
            caller_location={"lat": 20.8783, "lon": -156.6825},
            call_classification=classification
        )
        
        result = calculate_risk_score(input_data)
        assert isinstance(result, RiskScoreResult)
        assert 0.0 <= result.risk_score <= 100.0

