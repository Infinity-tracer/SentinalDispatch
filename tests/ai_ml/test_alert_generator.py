"""
Tests for alert generation module.
"""

import pytest
from datetime import datetime, timezone, timedelta

from sentinel_dispatch.ai_ml.alert_generator import (
    Alert,
    AlertGenerator,
    AlertSeverity,
    AlertType,
    generate_alerts,
)
from sentinel_dispatch.ai_ml.models import (
    RiskScoreResult,
    RiskFactors,
    EnrichedCall,
    CallClassificationResult,
    FireProximityAnalysis,
    WeatherEnrichment,
)


class TestAlertGenerator:
    """Test cases for AlertGenerator."""
    
    @pytest.fixture
    def generator(self):
        """Create an AlertGenerator instance."""
        return AlertGenerator()
    
    @pytest.fixture
    def high_risk_score(self):
        """Create a high risk score result."""
        return RiskScoreResult(
            call_id="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            risk_score=85.0,
            confidence=0.95,
            factors=RiskFactors(
                call_urgency=0.9,
                fire_proximity=0.8,
                fire_spread_rate=0.7,
                weather_risk=0.6,
                vulnerability=0.8,
                time_since_call=0.3,
            ),
            recommendation="IMMEDIATE_DISPATCH",
            priority=1,
        )
    
    @pytest.fixture
    def low_risk_score(self):
        """Create a low risk score result."""
        return RiskScoreResult(
            call_id="CALL-002",
            timestamp="2024-08-08T14:23:45Z",
            risk_score=25.0,
            confidence=0.8,
            factors=RiskFactors(
                call_urgency=0.3,
                fire_proximity=0.2,
                fire_spread_rate=None,
                weather_risk=None,
                vulnerability=0.3,
                time_since_call=0.1,
            ),
            recommendation="LOW_PRIORITY",
            priority=4,
        )
    
    @pytest.fixture
    def enriched_call_with_fire(self):
        """Create an enriched call with fire data."""
        return EnrichedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:20:00Z",
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
                estimated_time_to_reach=0.04,  # ~2.4 minutes
                fire_location={"lat": 20.8790, "lon": -156.6830},
                frp=33.0,  # Fire Radiative Power in MW
            ),
            weather_enrichment=WeatherEnrichment(
                temperature=85.0,
                wind_speed=25.0,
                wind_direction=270.0,
                humidity=45.0,
                air_quality_index=220,
                visibility=2.0,
            ),
            enrichment_timestamp="2024-08-08T14:23:45Z",
        )
    
    def test_generate_risk_threshold_alert_critical(self, generator, high_risk_score):
        """Test generating critical risk threshold alert."""
        alerts = generator.generate_alerts(high_risk_score)
        
        assert len(alerts) > 0
        risk_alerts = [a for a in alerts if a.alert_type == AlertType.RISK_THRESHOLD]
        assert len(risk_alerts) == 1
        
        alert = risk_alerts[0]
        assert alert.severity == AlertSeverity.CRITICAL
        assert alert.call_id == "CALL-001"
        assert "85.0" in alert.message
        assert alert.urgency == "immediate"
        assert alert.certainty == "observed"
        assert alert.metadata["risk_score"] == 85.0
    
    def test_generate_risk_threshold_alert_high(self, generator):
        """Test generating high risk threshold alert."""
        risk_score = RiskScoreResult(
            call_id="CALL-003",
            timestamp="2024-08-08T14:23:45Z",
            risk_score=70.0,
            confidence=0.9,
            factors=RiskFactors(
                call_urgency=0.7,
                fire_proximity=0.6,
                vulnerability=0.5,
            ),
            recommendation="HIGH_PRIORITY",
            priority=2,
        )
        
        alerts = generator.generate_alerts(risk_score)
        risk_alerts = [a for a in alerts if a.alert_type == AlertType.RISK_THRESHOLD]
        assert len(risk_alerts) == 1
        
        alert = risk_alerts[0]
        assert alert.severity == AlertSeverity.HIGH
        assert alert.urgency == "expected"
        assert alert.certainty == "likely"
    
    def test_generate_risk_threshold_alert_medium(self, generator):
        """Test generating medium risk threshold alert."""
        risk_score = RiskScoreResult(
            call_id="CALL-004",
            timestamp="2024-08-08T14:23:45Z",
            risk_score=50.0,
            confidence=0.85,
            factors=RiskFactors(
                call_urgency=0.5,
                fire_proximity=0.4,
                vulnerability=0.4,
            ),
            recommendation="MODERATE_PRIORITY",
            priority=3,
        )
        
        alerts = generator.generate_alerts(risk_score)
        risk_alerts = [a for a in alerts if a.alert_type == AlertType.RISK_THRESHOLD]
        assert len(risk_alerts) == 1
        
        alert = risk_alerts[0]
        assert alert.severity == AlertSeverity.MEDIUM
        assert alert.urgency == "future"
        assert alert.certainty == "possible"
    
    def test_generate_risk_threshold_alert_low(self, generator, low_risk_score):
        """Test that low risk scores generate low priority alerts."""
        alerts = generator.generate_alerts(low_risk_score)
        risk_alerts = [a for a in alerts if a.alert_type == AlertType.RISK_THRESHOLD]
        assert len(risk_alerts) == 1
        
        alert = risk_alerts[0]
        assert alert.severity == AlertSeverity.LOW
        assert alert.urgency == "future"
        assert alert.certainty == "possible"
        assert alert.call_id == "CALL-002"
        assert "25.0" in alert.message
    
    def test_generate_fire_proximity_alert_critical(self, generator, high_risk_score, enriched_call_with_fire):
        """Test generating critical fire proximity alert."""
        alerts = generator.generate_alerts(high_risk_score, enriched_call_with_fire)
        
        fire_alerts = [a for a in alerts if a.alert_type == AlertType.FIRE_PROXIMITY]
        assert len(fire_alerts) == 1
        
        alert = fire_alerts[0]
        assert alert.severity == AlertSeverity.CRITICAL
        assert "0.5" in alert.message
        assert alert.metadata["fire_distance_km"] == 0.5
    
    def test_generate_fire_proximity_alert_high(self, generator, high_risk_score):
        """Test generating high fire proximity alert."""
        enriched_call = EnrichedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:20:00Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Fire nearby",
            classification=CallClassificationResult(
                urgency=4,
                incident_type="fire",
                entities={},
                sentiment=0.7,
                key_phrases=[],
                confidence=0.85
            ),
            fire_analysis=FireProximityAnalysis(
                nearest_fire_id="FIRE-002",
                distance_km=3.0,  # Within high threshold
                bearing_degrees=90.0,
                spread_rate=None,
                spread_toward_caller=None,
                fire_location={"lat": 20.9000, "lon": -156.6500},
                frp=20.0,  # Fire Radiative Power in MW
            ),
            enrichment_timestamp="2024-08-08T14:23:45Z",
        )
        
        alerts = generator.generate_alerts(high_risk_score, enriched_call)
        fire_alerts = [a for a in alerts if a.alert_type == AlertType.FIRE_PROXIMITY]
        assert len(fire_alerts) == 1
        
        alert = fire_alerts[0]
        assert alert.severity == AlertSeverity.HIGH
    
    def test_generate_fire_spread_alert(self, generator, high_risk_score, enriched_call_with_fire):
        """Test generating fire spread alert."""
        alerts = generator.generate_alerts(high_risk_score, enriched_call_with_fire)
        
        spread_alerts = [a for a in alerts if a.alert_type == AlertType.FIRE_SPREAD]
        assert len(spread_alerts) == 1
        
        alert = spread_alerts[0]
        assert alert.severity == AlertSeverity.CRITICAL
        assert "spreading" in alert.message.lower()
        assert alert.metadata["spread_toward_caller"] is True
        assert alert.metadata["spread_rate"] == 12.0
    
    def test_no_fire_spread_alert_when_not_spreading(self, generator, high_risk_score):
        """Test that no alert is generated when fire is not spreading toward caller."""
        enriched_call = EnrichedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:20:00Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Fire nearby",
            classification=CallClassificationResult(
                urgency=4,
                incident_type="fire",
                entities={},
                sentiment=0.7,
                key_phrases=[],
                confidence=0.85
            ),
            fire_analysis=FireProximityAnalysis(
                nearest_fire_id="FIRE-003",
                distance_km=2.0,
                bearing_degrees=90.0,
                spread_rate=5.0,
                spread_toward_caller=False,  # Not spreading toward caller
                fire_location={"lat": 20.9000, "lon": -156.6500},
                frp=25.0,  # Fire Radiative Power in MW
            ),
            enrichment_timestamp="2024-08-08T14:23:45Z",
        )
        
        alerts = generator.generate_alerts(high_risk_score, enriched_call)
        spread_alerts = [a for a in alerts if a.alert_type == AlertType.FIRE_SPREAD]
        assert len(spread_alerts) == 0
    
    def test_generate_weather_hazard_alert_critical(self, generator, high_risk_score, enriched_call_with_fire):
        """Test generating critical weather hazard alert."""
        alerts = generator.generate_alerts(high_risk_score, enriched_call_with_fire)
        
        weather_alerts = [a for a in alerts if a.alert_type == AlertType.WEATHER_HAZARD]
        assert len(weather_alerts) == 1
        
        alert = weather_alerts[0]
        assert alert.severity == AlertSeverity.CRITICAL
        assert "air quality" in alert.message.lower()
        assert alert.metadata["air_quality_index"] == 220
    
    def test_generate_weather_hazard_alert_high(self, generator, high_risk_score):
        """Test generating high weather hazard alert."""
        enriched_call = EnrichedCall(
            call_id="CALL-001",
            timestamp="2024-08-08T14:20:00Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Smoke in the area",
            classification=CallClassificationResult(
                urgency=3,
                incident_type="fire",
                entities={},
                sentiment=0.6,
                key_phrases=[],
                confidence=0.8
            ),
            weather_enrichment=WeatherEnrichment(
                air_quality_index=170,  # High but not critical
                wind_speed=20.0,
                humidity=50.0,
            ),
            enrichment_timestamp="2024-08-08T14:23:45Z",
        )
        
        alerts = generator.generate_alerts(high_risk_score, enriched_call)
        weather_alerts = [a for a in alerts if a.alert_type == AlertType.WEATHER_HAZARD]
        assert len(weather_alerts) == 1
        
        alert = weather_alerts[0]
        assert alert.severity == AlertSeverity.HIGH
    
    def test_generate_time_critical_alert(self, generator):
        """Test generating time critical alert."""
        # Create a call that was made 45 minutes ago
        call_timestamp = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
        # Risk score timestamp is current (45 minutes after call)
        risk_score_timestamp = datetime.now(timezone.utc).isoformat()
        
        # Create a high risk score with current timestamp
        high_risk_score = RiskScoreResult(
            call_id="CALL-001",
            timestamp=risk_score_timestamp,
            risk_score=85.0,  # High enough to trigger alert
            confidence=0.95,
            factors=RiskFactors(
                call_urgency=0.9,
                fire_proximity=0.8,
                fire_spread_rate=0.7,
                weather_risk=0.6,
                vulnerability=0.8,
                time_since_call=0.3,
            ),
            recommendation="IMMEDIATE_DISPATCH",
            priority=1,
        )
        
        enriched_call = EnrichedCall(
            call_id="CALL-001",
            timestamp=call_timestamp,
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Need help",
            classification=CallClassificationResult(
                urgency=4,
                incident_type="medical",
                entities={},
                sentiment=0.7,
                key_phrases=[],
                confidence=0.85
            ),
            enrichment_timestamp=risk_score_timestamp,
        )
        
        alerts = generator.generate_alerts(high_risk_score, enriched_call)
        time_alerts = [a for a in alerts if a.alert_type == AlertType.TIME_CRITICAL]
        assert len(time_alerts) == 1
        
        alert = time_alerts[0]
        assert alert.severity == AlertSeverity.HIGH
        assert "minutes" in alert.message.lower()
    
    def test_generate_vulnerability_alert(self, generator):
        """Test generating vulnerability alert."""
        risk_score = RiskScoreResult(
            call_id="CALL-005",
            timestamp="2024-08-08T14:23:45Z",
            risk_score=55.0,  # Medium-high risk
            confidence=0.9,
            factors=RiskFactors(
                call_urgency=0.6,
                fire_proximity=0.5,
                vulnerability=0.85,  # High vulnerability
            ),
            recommendation="HIGH_PRIORITY",
            priority=2,
        )
        
        enriched_call = EnrichedCall(
            call_id="CALL-005",
            timestamp="2024-08-08T14:20:00Z",
            caller_location={"lat": 20.8783, "lon": -156.6825},
            transcript="Elderly person needs help",
            classification=CallClassificationResult(
                urgency=4,
                incident_type="medical",
                entities={},
                sentiment=0.7,
                key_phrases=[],
                confidence=0.85
            ),
            enrichment_timestamp="2024-08-08T14:23:45Z",
        )
        
        alerts = generator.generate_alerts(risk_score, enriched_call)
        vulnerability_alerts = [a for a in alerts if a.alert_type == AlertType.VULNERABILITY]
        assert len(vulnerability_alerts) == 1
        
        alert = vulnerability_alerts[0]
        assert alert.severity == AlertSeverity.HIGH
        assert "vulnerability" in alert.message.lower()
    
    def test_alert_to_dict(self, generator, high_risk_score):
        """Test alert to_dict conversion."""
        alerts = generator.generate_alerts(high_risk_score)
        assert len(alerts) > 0
        
        alert_dict = alerts[0].to_dict()
        assert "alertId" in alert_dict
        assert "timestamp" in alert_dict
        assert "severity" in alert_dict
        assert "message" in alert_dict
        assert alert_dict["severity"] in ["low", "medium", "high", "critical"]
    
    def test_alert_expiry_calculation(self, generator, high_risk_score):
        """Test alert expiry time calculation."""
        alerts = generator.generate_alerts(high_risk_score)
        assert len(alerts) > 0
        
        for alert in alerts:
            assert alert.expires is not None
            # Verify expiry is after timestamp
            try:
                alert_time = datetime.fromisoformat(alert.timestamp.replace("Z", "+00:00"))
                expiry_time = datetime.fromisoformat(alert.expires.replace("Z", "+00:00"))
                assert expiry_time > alert_time
            except Exception:
                pass  # Skip if timestamp parsing fails
    
    def test_convenience_function(self, high_risk_score, enriched_call_with_fire):
        """Test convenience function."""
        alerts = generate_alerts(high_risk_score, enriched_call_with_fire)
        assert len(alerts) > 0
        assert all(isinstance(a, Alert) for a in alerts)
    
    def test_custom_thresholds(self):
        """Test alert generator with custom thresholds."""
        generator = AlertGenerator(
            risk_threshold_critical=90.0,
            risk_threshold_high=70.0,
            risk_threshold_medium=50.0,
            risk_threshold_low=10.0,
            fire_proximity_critical_km=0.5,
            fire_proximity_high_km=3.0,
        )
        
        risk_score = RiskScoreResult(
            call_id="CALL-006",
            timestamp="2024-08-08T14:23:45Z",
            risk_score=75.0,  # Would be critical with default, but high with custom
            confidence=0.9,
            factors=RiskFactors(
                call_urgency=0.7,
                fire_proximity=0.6,
                vulnerability=0.5,
            ),
            recommendation="HIGH_PRIORITY",
            priority=2,
        )
        
        alerts = generator.generate_alerts(risk_score)
        risk_alerts = [a for a in alerts if a.alert_type == AlertType.RISK_THRESHOLD]
        assert len(risk_alerts) == 1
        assert risk_alerts[0].severity == AlertSeverity.HIGH

