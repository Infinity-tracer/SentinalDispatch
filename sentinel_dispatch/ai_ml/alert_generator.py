"""
Alert Generation Module for Emergency Dispatch.

This module generates alerts based on risk scores, fire proximity, weather conditions,
and other factors to notify dispatchers of critical situations requiring immediate attention.

Features:
- Threshold-based alert generation
- Alert deduplication
- Severity classification
- Configurable alert rules
"""

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from enum import Enum

from sentinel_dispatch.ai_ml.models import (
    RiskScoreResult,
    EnrichedCall,
)
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Alert type categories."""
    
    RISK_THRESHOLD = "risk_threshold"
    FIRE_PROXIMITY = "fire_proximity"
    FIRE_SPREAD = "fire_spread"
    WEATHER_HAZARD = "weather_hazard"
    URGENCY_ESCALATION = "urgency_escalation"
    TIME_CRITICAL = "time_critical"
    VULNERABILITY = "vulnerability"


class Alert:
    """Alert model for emergency dispatch notifications."""
    
    def __init__(
        self,
        alert_id: str,
        timestamp: str,
        severity: AlertSeverity,
        message: str,
        alert_type: AlertType,
        call_id: Optional[str] = None,
        event: Optional[str] = None,
        headline: Optional[str] = None,
        description: Optional[str] = None,
        urgency: Optional[str] = None,
        certainty: Optional[str] = None,
        effective: Optional[str] = None,
        expires: Optional[str] = None,
        status: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize an alert.
        
        Args:
            alert_id: Unique alert identifier
            timestamp: Alert timestamp (ISO format)
            severity: Alert severity level
            message: Alert message
            alert_type: Type of alert
            call_id: Associated call ID (if applicable)
            event: Event type
            headline: Alert headline
            description: Detailed description
            urgency: Urgency level
            certainty: Certainty level
            effective: Effective time (ISO format)
            expires: Expiration time (ISO format)
            status: Alert status
            metadata: Additional metadata
        """
        self.alert_id = alert_id
        self.timestamp = timestamp
        self.severity = severity
        self.message = message
        self.alert_type = alert_type
        self.call_id = call_id
        self.event = event
        self.headline = headline
        self.description = description
        self.urgency = urgency
        self.certainty = certainty
        self.effective = effective
        self.expires = expires
        self.status = status or "active"
        self.metadata = metadata or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alertId": self.alert_id,
            "timestamp": self.timestamp,
            "severity": self.severity.value,
            "message": self.message,
            "alert_type": self.alert_type.value,
            "callId": self.call_id,
            "event": self.event,
            "headline": self.headline,
            "description": self.description,
            "urgency": self.urgency,
            "certainty": self.certainty,
            "effective": self.effective,
            "expires": self.expires,
            "status": self.status,
            "metadata": self.metadata,
        }


class AlertGenerator:
    """Generates alerts based on risk scores and contextual data."""
    
    def __init__(
        self,
        risk_threshold_critical: float = 80.0,
        risk_threshold_high: float = 60.0,
        risk_threshold_medium: float = 40.0,
        risk_threshold_low: float = 0.0,
        fire_proximity_critical_km: float = 1.0,
        fire_proximity_high_km: float = 5.0,
        fire_spread_rate_critical: float = 10.0,
        time_critical_minutes: int = 30,
        air_quality_critical: int = 200,
        air_quality_high: int = 150,
    ):
        """
        Initialize the alert generator.
        
        Args:
            risk_threshold_critical: Risk score threshold for critical alerts (default: 80.0)
            risk_threshold_high: Risk score threshold for high alerts (default: 60.0)
            risk_threshold_medium: Risk score threshold for medium alerts (default: 40.0)
            risk_threshold_low: Risk score threshold for low alerts (default: 0.0, generates alerts for all risk scores)
            fire_proximity_critical_km: Fire proximity for critical alerts in km (default: 1.0)
            fire_proximity_high_km: Fire proximity for high alerts in km (default: 5.0)
            fire_spread_rate_critical: Fire spread rate for critical alerts in km/h (default: 10.0)
            time_critical_minutes: Time since call for time-critical alerts in minutes (default: 30)
            air_quality_critical: Air quality index for critical alerts (default: 200)
            air_quality_high: Air quality index for high alerts (default: 150)
        """
        self.risk_threshold_critical = risk_threshold_critical
        self.risk_threshold_high = risk_threshold_high
        self.risk_threshold_medium = risk_threshold_medium
        self.risk_threshold_low = risk_threshold_low
        self.fire_proximity_critical_km = fire_proximity_critical_km
        self.fire_proximity_high_km = fire_proximity_high_km
        self.fire_spread_rate_critical = fire_spread_rate_critical
        self.time_critical_minutes = time_critical_minutes
        self.air_quality_critical = air_quality_critical
        self.air_quality_high = air_quality_high
        
        logger.info(
            "sentinel_dispatch.ai_ml.alert_generator.AlertGenerator.__init__.initialized",
            risk_threshold_critical=risk_threshold_critical,
            risk_threshold_high=risk_threshold_high,
            risk_threshold_medium=risk_threshold_medium,
            risk_threshold_low=risk_threshold_low
        )
    
    def generate_alerts(
        self,
        risk_score: RiskScoreResult,
        enriched_call: Optional[EnrichedCall] = None,
    ) -> List[Alert]:
        """
        Generate alerts based on risk score and enriched call data.
        
        Args:
            risk_score: Risk score result
            enriched_call: Enriched call data (optional)
            
        Returns:
            List of generated alerts
        """
        alerts = []
        
        # Generate risk threshold alert
        risk_alert = self._generate_risk_threshold_alert(risk_score)
        if risk_alert:
            alerts.append(risk_alert)
        
        if enriched_call:
            # Generate fire proximity alert
            fire_proximity_alert = self._generate_fire_proximity_alert(
                risk_score, enriched_call
            )
            if fire_proximity_alert:
                alerts.append(fire_proximity_alert)
            
            # Generate fire spread alert
            fire_spread_alert = self._generate_fire_spread_alert(
                risk_score, enriched_call
            )
            if fire_spread_alert:
                alerts.append(fire_spread_alert)
            
            # Generate weather hazard alert
            weather_alert = self._generate_weather_hazard_alert(
                risk_score, enriched_call
            )
            if weather_alert:
                alerts.append(weather_alert)
            
            # Generate time critical alert
            time_alert = self._generate_time_critical_alert(
                risk_score, enriched_call
            )
            if time_alert:
                alerts.append(time_alert)
            
            # Generate vulnerability alert
            vulnerability_alert = self._generate_vulnerability_alert(
                risk_score, enriched_call
            )
            if vulnerability_alert:
                alerts.append(vulnerability_alert)
        
        logger.debug(
            "sentinel_dispatch.ai_ml.alert_generator.AlertGenerator.generate_alerts.completed",
            call_id=risk_score.call_id,
            alert_count=len(alerts)
        )
        
        return alerts
    
    def _generate_risk_threshold_alert(
        self, risk_score: RiskScoreResult
    ) -> Optional[Alert]:
        """Generate alert based on risk score threshold."""
        if risk_score.risk_score >= self.risk_threshold_critical:
            severity = AlertSeverity.CRITICAL
            urgency = "immediate"
            certainty = "observed"
            severity_label = "Critical"
        elif risk_score.risk_score >= self.risk_threshold_high:
            severity = AlertSeverity.HIGH
            urgency = "expected"
            certainty = "likely"
            severity_label = "High"
        elif risk_score.risk_score >= self.risk_threshold_medium:
            severity = AlertSeverity.MEDIUM
            urgency = "future"
            certainty = "possible"
            severity_label = "Medium"
        elif risk_score.risk_score >= self.risk_threshold_low:
            severity = AlertSeverity.LOW
            urgency = "future"
            certainty = "possible"
            severity_label = "Low"
        else:
            return None  # Risk score below minimum threshold
        
        # Dynamic message based on severity
        message = (
            f"{severity_label} risk score detected: {risk_score.risk_score:.1f}/100. "
            f"Recommendation: {risk_score.recommendation}"
        )
        headline = f"{severity_label} Risk Score Alert: {risk_score.risk_score:.1f}/100"
        description = (
            f"Emergency call {risk_score.call_id} has a {severity_label.lower()} risk score of "
            f"{risk_score.risk_score:.1f}/100 with {risk_score.recommendation} recommendation. "
            f"Priority level: {risk_score.priority}"
        )
        
        return Alert(
            alert_id=str(uuid.uuid4()),
            timestamp=risk_score.timestamp,
            severity=severity,
            message=message,
            alert_type=AlertType.RISK_THRESHOLD,
            call_id=risk_score.call_id,
            headline=headline,
            description=description,
            urgency=urgency,
            certainty=certainty,
            effective=risk_score.timestamp,
            expires=self._calculate_expiry(risk_score.timestamp, severity),
            metadata={
                "risk_score": risk_score.risk_score,
                "confidence": risk_score.confidence,
                "recommendation": risk_score.recommendation,
                "priority": risk_score.priority,
            },
        )
    
    def _generate_fire_proximity_alert(
        self, risk_score: RiskScoreResult, enriched_call: EnrichedCall
    ) -> Optional[Alert]:
        """Generate alert based on fire proximity."""
        if not enriched_call.fire_analysis:
            return None
        
        fire_analysis = enriched_call.fire_analysis
        distance_km = fire_analysis.distance_km
        
        if distance_km is None:
            return None
        
        if distance_km <= self.fire_proximity_critical_km:
            severity = AlertSeverity.CRITICAL
            urgency = "immediate"
            certainty = "observed"
            message = (
                f"CRITICAL: Fire is extremely close ({distance_km:.2f} km) to caller location. "
                f"Immediate evacuation may be required."
            )
        elif distance_km <= self.fire_proximity_high_km:
            severity = AlertSeverity.HIGH
            urgency = "expected"
            certainty = "likely"
            message = (
                f"Fire is nearby ({distance_km:.2f} km) to caller location. "
                f"Monitor closely and prepare for evacuation."
            )
        else:
            return None  # Fire is far enough, no alert
        
        headline = f"Fire Proximity Alert: {distance_km:.2f} km"
        description = (
            f"Fire detected {distance_km:.2f} km from caller location. "
            f"Bearing: {fire_analysis.bearing_degrees:.1f}°"
        )
        
        return Alert(
            alert_id=str(uuid.uuid4()),
            timestamp=risk_score.timestamp,
            severity=severity,
            message=message,
            alert_type=AlertType.FIRE_PROXIMITY,
            call_id=risk_score.call_id,
            headline=headline,
            description=description,
            urgency=urgency,
            certainty=certainty,
            effective=risk_score.timestamp,
            expires=self._calculate_expiry(risk_score.timestamp, severity),
            metadata={
                "fire_distance_km": distance_km,
                "bearing_degrees": fire_analysis.bearing_degrees,
                "fire_id": fire_analysis.nearest_fire_id,
            },
        )
    
    def _generate_fire_spread_alert(
        self, risk_score: RiskScoreResult, enriched_call: EnrichedCall
    ) -> Optional[Alert]:
        """Generate alert based on fire spread rate and direction."""
        if not enriched_call.fire_analysis:
            return None
        
        fire_analysis = enriched_call.fire_analysis
        
        if not fire_analysis.spread_toward_caller:
            return None  # Fire not spreading toward caller
        
        spread_rate = fire_analysis.spread_rate
        if spread_rate is None or spread_rate < self.fire_spread_rate_critical:
            return None
        
        estimated_time = fire_analysis.estimated_time_to_reach
        if estimated_time is None:
            return None
        
        severity = AlertSeverity.CRITICAL
        urgency = "immediate"
        certainty = "observed"
        
        message = (
            f"CRITICAL: Fire is spreading toward caller at {spread_rate:.1f} km/h. "
            f"Estimated time to reach: {estimated_time:.1f} hours."
        )
        headline = f"Fire Spreading Toward Caller: {spread_rate:.1f} km/h"
        description = (
            f"Fire is actively spreading toward caller location at {spread_rate:.1f} km/h. "
            f"Estimated time to reach caller: {estimated_time:.1f} hours. "
            f"Immediate evacuation recommended."
        )
        
        return Alert(
            alert_id=str(uuid.uuid4()),
            timestamp=risk_score.timestamp,
            severity=severity,
            message=message,
            alert_type=AlertType.FIRE_SPREAD,
            call_id=risk_score.call_id,
            headline=headline,
            description=description,
            urgency=urgency,
            certainty=certainty,
            effective=risk_score.timestamp,
            expires=self._calculate_expiry(risk_score.timestamp, severity),
            metadata={
                "spread_rate": spread_rate,
                "estimated_time_to_reach": estimated_time,
                "spread_toward_caller": True,
            },
        )
    
    def _generate_weather_hazard_alert(
        self, risk_score: RiskScoreResult, enriched_call: EnrichedCall
    ) -> Optional[Alert]:
        """
        Generate alert based on weather hazards.
        
        Note: Air quality index (AQI) is optional and requires AirNow API integration
        (deferred to future enhancement). This alert only triggers if AQI data is available.
        """
        if not enriched_call.weather_enrichment:
            return None
        
        weather = enriched_call.weather_enrichment
        air_quality = weather.air_quality_index
        
        # Air quality is optional - only generate alert if data is available
        if air_quality is None:
            return None
        
        if air_quality >= self.air_quality_critical:
            severity = AlertSeverity.CRITICAL
            urgency = "immediate"
            certainty = "observed"
            message = (
                f"CRITICAL: Extremely poor air quality (AQI: {air_quality}). "
                f"Health hazard for vulnerable individuals."
            )
        elif air_quality >= self.air_quality_high:
            severity = AlertSeverity.HIGH
            urgency = "expected"
            certainty = "likely"
            message = (
                f"Poor air quality (AQI: {air_quality}). "
                f"Monitor vulnerable individuals closely."
            )
        else:
            return None  # Air quality is acceptable
        
        headline = f"Weather Hazard Alert: AQI {air_quality}"
        description = (
            f"Air quality index is {air_quality}, indicating hazardous conditions. "
            f"Wind speed: {weather.wind_speed} mph, "
            f"Humidity: {weather.humidity}%"
        )
        
        return Alert(
            alert_id=str(uuid.uuid4()),
            timestamp=risk_score.timestamp,
            severity=severity,
            message=message,
            alert_type=AlertType.WEATHER_HAZARD,
            call_id=risk_score.call_id,
            headline=headline,
            description=description,
            urgency=urgency,
            certainty=certainty,
            effective=risk_score.timestamp,
            expires=self._calculate_expiry(risk_score.timestamp, severity),
            metadata={
                "air_quality_index": air_quality,
                "wind_speed": weather.wind_speed,
                "wind_direction": weather.wind_direction,
                "humidity": weather.humidity,
            },
        )
    
    def _generate_time_critical_alert(
        self, risk_score: RiskScoreResult, enriched_call: EnrichedCall
    ) -> Optional[Alert]:
        """Generate alert if call has been waiting too long."""
        try:
            call_time = datetime.fromisoformat(
                enriched_call.timestamp.replace("Z", "+00:00")
            )
            current_time = datetime.fromisoformat(
                risk_score.timestamp.replace("Z", "+00:00")
            )
            time_diff = (current_time - call_time).total_seconds() / 60.0  # minutes
        except Exception:
            return None
        
        if time_diff < self.time_critical_minutes:
            return None  # Not enough time has passed
        
        # Only alert if risk score is also high
        if risk_score.risk_score < self.risk_threshold_high:
            return None
        
        severity = AlertSeverity.HIGH
        urgency = "immediate"
        certainty = "observed"
        
        message = (
            f"Call has been waiting for {time_diff:.0f} minutes with high risk score. "
            f"Immediate dispatch recommended."
        )
        headline = f"Time Critical Alert: {time_diff:.0f} minutes"
        description = (
            f"Emergency call {risk_score.call_id} has been waiting for "
            f"{time_diff:.0f} minutes with a risk score of {risk_score.risk_score:.1f}/100. "
            f"Immediate response recommended."
        )
        
        return Alert(
            alert_id=str(uuid.uuid4()),
            timestamp=risk_score.timestamp,
            severity=severity,
            message=message,
            alert_type=AlertType.TIME_CRITICAL,
            call_id=risk_score.call_id,
            headline=headline,
            description=description,
            urgency=urgency,
            certainty=certainty,
            effective=risk_score.timestamp,
            expires=self._calculate_expiry(risk_score.timestamp, severity),
            metadata={
                "time_since_call_minutes": time_diff,
                "risk_score": risk_score.risk_score,
            },
        )
    
    def _generate_vulnerability_alert(
        self, risk_score: RiskScoreResult, enriched_call: EnrichedCall
    ) -> Optional[Alert]:
        """Generate alert for high vulnerability cases."""
        # Check if vulnerability factor is high
        vulnerability_factor = risk_score.factors.vulnerability
        
        if vulnerability_factor < 0.7:
            return None  # Not high enough vulnerability
        
        # Only alert if risk score is also medium or higher
        if risk_score.risk_score < self.risk_threshold_medium:
            return None
        
        severity = AlertSeverity.HIGH
        urgency = "expected"
        certainty = "likely"
        
        message = (
            f"High vulnerability case detected (vulnerability factor: {vulnerability_factor:.2f}). "
            f"Prioritize response for vulnerable individuals."
        )
        headline = "Vulnerability Alert: High Risk Individual"
        description = (
            f"Emergency call {risk_score.call_id} involves individuals with high vulnerability. "
            f"Risk score: {risk_score.risk_score:.1f}/100. "
            f"Immediate attention recommended."
        )
        
        return Alert(
            alert_id=str(uuid.uuid4()),
            timestamp=risk_score.timestamp,
            severity=severity,
            message=message,
            alert_type=AlertType.VULNERABILITY,
            call_id=risk_score.call_id,
            headline=headline,
            description=description,
            urgency=urgency,
            certainty=certainty,
            effective=risk_score.timestamp,
            expires=self._calculate_expiry(risk_score.timestamp, severity),
            metadata={
                "vulnerability_factor": vulnerability_factor,
                "risk_score": risk_score.risk_score,
            },
        )
    
    def _calculate_expiry(
        self, timestamp: str, severity: AlertSeverity
    ) -> str:
        """
        Calculate alert expiration time based on severity.
        
        Args:
            timestamp: Alert timestamp (ISO format)
            severity: Alert severity
            
        Returns:
            Expiration timestamp (ISO format)
        """
        try:
            alert_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except Exception:
            alert_time = datetime.now(timezone.utc)
        
        # Expiry times based on severity
        expiry_hours = {
            AlertSeverity.CRITICAL: 1,  # 1 hour
            AlertSeverity.HIGH: 2,  # 2 hours
            AlertSeverity.MEDIUM: 4,  # 4 hours
            AlertSeverity.LOW: 8,  # 8 hours
        }
        
        expiry_time = alert_time + timedelta(hours=expiry_hours.get(severity, 4))
        return expiry_time.isoformat()


# Convenience function
def generate_alerts(
    risk_score: RiskScoreResult,
    enriched_call: Optional[EnrichedCall] = None,
) -> List[Alert]:
    """
    Convenience function to generate alerts.
    
    Args:
        risk_score: Risk score result
        enriched_call: Enriched call data (optional)
        
    Returns:
        List of generated alerts
    """
    generator = AlertGenerator()
    return generator.generate_alerts(risk_score, enriched_call)

