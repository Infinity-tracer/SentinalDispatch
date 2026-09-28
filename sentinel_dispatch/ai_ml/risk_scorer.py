"""
Risk Scoring Module for Emergency Dispatch.

This module calculates dynamic risk scores for emergency calls based on multiple factors:
- Call urgency (from NLP classification)
- Fire proximity and spread rate
- Weather conditions
- Vulnerability factors (age, medical conditions)
- Time since call

Uses a hybrid approach: automatically tries Vertex AI ML models if available and configured,
otherwise falls back to rule-based algorithm.
"""

from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

import structlog

from sentinel_dispatch.ai_ml.models import (
    CallClassificationResult,
    RiskFactors,
    RiskScoreInput,
    RiskScoreResult,
)
from sentinel_dispatch.ai_ml.risk_utils import create_risk_score_result
from sentinel_dispatch.utils.geospatial import haversine_distance

if TYPE_CHECKING:
    from sentinel_dispatch.ai_ml.vertex_ai_scorer import VertexAIRiskScorer

logger = structlog.get_logger(__name__)



class RiskScorer:
    """Calculates risk scores for emergency calls using hybrid approach."""
    
    def __init__(self, vertex_ai_scorer: Optional["VertexAIRiskScorer"] = None):
        """
        Initialize the risk scorer with hybrid mode (Vertex AI if available, else rule-based).
        
        Args:
            vertex_ai_scorer: Optional Vertex AI scorer instance (auto-initialized if available)
        """
        # Factor weights for rule-based calculation
        self.weights = {
            "call_urgency": 0.30,  # 30% weight
            "fire_proximity": 0.25,  # 25% weight
            "fire_spread_rate": 0.15,  # 15% weight
            "weather_risk": 0.10,  # 10% weight
            "vulnerability": 0.15,  # 15% weight
            "time_since_call": 0.05,  # 5% weight
        }
        
        # Initialize Vertex AI scorer if available
        self.vertex_ai_scorer = vertex_ai_scorer
        if not self.vertex_ai_scorer:
            try:
                self.vertex_ai_scorer = VertexAIRiskScorer()
                if not self.vertex_ai_scorer.is_available():
                    logger.info(
                        "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.__init__.vertex_ai_not_configured",
                        message="Vertex AI not configured (missing env vars), using rule-based"
                    )
                    self.vertex_ai_scorer = None
            except (ValueError, ImportError) as e:
                # Expected errors: missing env vars, missing package
                logger.info(
                    "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.__init__.vertex_ai_unavailable",
                    error=str(e),
                    message="Vertex AI unavailable, using rule-based fallback"
                )
                self.vertex_ai_scorer = None
            except Exception as e:
                # Unexpected errors during initialization
                logger.warning(
                    "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.__init__.vertex_ai_init_failed",
                    error=str(e),
                    message="Vertex AI initialization failed, using rule-based fallback"
                )
                self.vertex_ai_scorer = None
        
        logger.info(
            "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.__init__.initialized",
            weights=self.weights,
            vertex_ai_available=self.vertex_ai_scorer is not None
        )
    
    def calculate_call_urgency_factor(
        self, classification: CallClassificationResult, phase: Optional[str] = None
    ) -> float:
        """
        Calculate call urgency factor (0.0-1.0) from classification.
        
        Args:
            classification: Call classification result
            phase: Emergency phase ('early', 'developing', 'critical', 'peak', 'aftermath')
            
        Returns:
            Urgency factor normalized to 0.0-1.0
        """
        # Urgency is 1-5, normalize to 0.0-1.0
        urgency_normalized = (classification.urgency - 1) / 4.0
        
        # Boost by sentiment (panic level)
        sentiment_boost = classification.sentiment * 0.2
        
        # Boost by phase if peak or critical (indicates high-stress emergency period)
        phase_boost = 0.0
        if phase:
            phase_lower = phase.lower()
            if phase_lower == "peak":
                phase_boost = 0.15  # Peak emergency period - highest urgency
            elif phase_lower == "critical":
                phase_boost = 0.10  # Critical period - high urgency
            elif phase_lower == "developing":
                phase_boost = 0.05  # Developing situation - moderate boost
            # early and aftermath get no boost
        
        # Combine with cap at 1.0
        factor = min(1.0, urgency_normalized + sentiment_boost + phase_boost)
        
        return factor
    
    def calculate_fire_proximity_factor(
        self, caller_location: dict, nearest_fire: Optional[dict]
    ) -> float:
        """
        Calculate fire proximity risk factor (0.0-1.0).
        
        Args:
            caller_location: {"lat": float, "lon": float}
            nearest_fire: Fire data with location and distance_km
            
        Returns:
            Fire proximity factor (0.0=far, 1.0=immediate danger)
        """
        if not nearest_fire:
            return 0.0
        
        # Get distance in km
        distance_km = nearest_fire.get("distance_km")
        if distance_km is None:
            # Calculate distance if not provided
            fire_lat = nearest_fire.get("location", {}).get("lat")
            fire_lon = nearest_fire.get("location", {}).get("lon")
            if fire_lat and fire_lon:
                distance_km = haversine_distance(
                    caller_location["lat"],
                    caller_location["lon"],
                    fire_lat,
                    fire_lon,
                )
            else:
                return 0.0
        
        # Convert distance to risk factor
        # < 1km = 1.0, 1-5km = 0.8, 5-10km = 0.5, 10-20km = 0.2, >20km = 0.0
        if distance_km < 1.0:
            return 1.0
        elif distance_km < 5.0:
            return 0.8 + (1.0 - distance_km / 5.0) * 0.2
        elif distance_km < 10.0:
            return 0.5 + (5.0 - distance_km / 5.0) * 0.3
        elif distance_km < 20.0:
            return 0.2 + (10.0 - distance_km / 10.0) * 0.3
        else:
            return max(0.0, 0.2 - (distance_km - 20.0) / 100.0)
    
    def calculate_fire_spread_rate_factor(
        self, caller_location: dict, nearest_fire: Optional[dict]
    ) -> float:
        """
        Calculate fire spread rate factor (0.0-1.0) using Fire Radiative Power (FRP).
        
        FRP (Fire Radiative Power) from FIRMS API is measured in MW (megawatts).
        Higher FRP indicates more intense fires that can spread faster.
        
        Args:
            caller_location: {"lat": float, "lon": float}
            nearest_fire: Fire data with frp (Fire Radiative Power in MW), spreadRate, etc.
            
        Returns:
            Fire spread rate factor (0.0=low intensity/not spreading, 1.0=high intensity/rapid spread)
        """
        if not nearest_fire:
            return 0.0
        
        # Use FRP (Fire Radiative Power) from FIRMS API as primary indicator
        # FRP is in MW (megawatts) - higher values indicate more intense fires
        frp = nearest_fire.get("frp") or nearest_fire.get("FRP")  # MW
        
        # Fallback to spreadRate if frp is not available
        spread_rate = None
        if frp is None or frp == 0.0:
            spread_rate = nearest_fire.get("spread_rate") or nearest_fire.get("spreadRate")  # km/h
        
        if frp is not None and frp > 0:
            # FRP normalization: 
            # - 0-10 MW = low intensity (0.0-0.3)
            # - 10-50 MW = moderate intensity (0.3-0.7)
            # - 50-100 MW = high intensity (0.7-0.9)
            # - 100+ MW = very high intensity (0.9-1.0)
            if frp < 10:
                factor = frp / 10.0 * 0.3
            elif frp < 50:
                factor = 0.3 + (frp - 10) / 40.0 * 0.4
            elif frp < 100:
                factor = 0.7 + (frp - 50) / 50.0 * 0.2
            else:
                factor = 0.9 + min(0.1, (frp - 100) / 200.0 * 0.1)
            
            return min(1.0, factor)
        elif spread_rate is not None and spread_rate > 0:
            # Fallback to spread_rate if frp not available
            # Normalize spread rate: 0 km/h = 0.0, 10+ km/h = 1.0
            factor = min(1.0, spread_rate / 10.0)
            return factor
        else:
            return 0.0
    
    def calculate_weather_risk_factor(
        self, weather_data: Optional[dict]
    ) -> float:
        """
        Calculate weather risk factor (0.0-1.0).
        
        Args:
            weather_data: Weather data with wind_speed, air_quality_index, etc.
            
        Returns:
            Weather risk factor (0.0=low risk, 1.0=high risk)
        """
        if not weather_data:
            return 0.0
        
        risk_components = []
        
        # Wind speed risk (high wind = higher fire spread risk)
        wind_speed = weather_data.get("wind_speed")  # mph or m/s
        if wind_speed:
            # Normalize: 0-20 mph = 0.0-0.5, 20-40 mph = 0.5-0.8, 40+ mph = 0.8-1.0
            if wind_speed < 20:
                wind_risk = wind_speed / 20.0 * 0.5
            elif wind_speed < 40:
                wind_risk = 0.5 + (wind_speed - 20) / 20.0 * 0.3
            else:
                wind_risk = 0.8 + min(0.2, (wind_speed - 40) / 40.0 * 0.2)
            risk_components.append(wind_risk)
        
        # Air quality risk (smoke, poor air quality)
        air_quality = weather_data.get("air_quality_index")
        if air_quality:
            # AQI: 0-50 (good) = 0.0, 50-100 (moderate) = 0.2, 100-150 (unhealthy) = 0.5,
            # 150-200 (unhealthy) = 0.7, 200+ (hazardous) = 1.0
            if air_quality < 50:
                aqi_risk = 0.0
            elif air_quality < 100:
                aqi_risk = 0.2
            elif air_quality < 150:
                aqi_risk = 0.5
            elif air_quality < 200:
                aqi_risk = 0.7
            else:
                aqi_risk = 1.0
            risk_components.append(aqi_risk)
        
        # Humidity risk (low humidity = higher fire risk)
        humidity = weather_data.get("humidity")  # percentage
        if humidity is not None:
            # Low humidity (<30%) = higher risk, high humidity (>70%) = lower risk
            if humidity < 30:
                humidity_risk = 0.8
            elif humidity < 50:
                humidity_risk = 0.5
            elif humidity < 70:
                humidity_risk = 0.2
            else:
                humidity_risk = 0.0
            risk_components.append(humidity_risk)
        
        # Average the risk components
        if risk_components:
            return sum(risk_components) / len(risk_components)
        else:
            return 0.0
    
    def calculate_vulnerability_factor(
        self, vulnerability_factors: Optional[dict]
    ) -> float:
        """
        Calculate vulnerability factor (0.0-1.0) based on age, medical conditions, mobility.
        
        Args:
            vulnerability_factors: {"age": int, "medical_conditions": [str], "mobility": str}
            
        Returns:
            Vulnerability factor (0.0=low vulnerability, 1.0=high vulnerability)
        """
        if not vulnerability_factors:
            return 0.3  # Default moderate vulnerability
        
        vulnerability_score = 0.0
        
        # Age factor
        age = vulnerability_factors.get("age")
        if age:
            if age < 18:  # Children
                vulnerability_score += 0.3
            elif age >= 65:  # Elderly
                vulnerability_score += 0.4
            elif age >= 75:  # Very elderly
                vulnerability_score += 0.5
        
        # Medical conditions
        conditions = vulnerability_factors.get("medical_conditions", [])
        if conditions:
            # High-risk conditions
            high_risk_conditions = [
                "asthma",
                "copd",
                "heart_disease",
                "diabetes",
                "respiratory",
            ]
            condition_count = sum(
                1 for c in conditions if any(hr in c.lower() for hr in high_risk_conditions)
            )
            vulnerability_score += min(0.4, condition_count * 0.2)
        
        # Mobility
        mobility = vulnerability_factors.get("mobility", "normal")
        if mobility == "limited":
            vulnerability_score += 0.3
        elif mobility == "wheelchair" or mobility == "immobile":
            vulnerability_score += 0.5
        
        # Cap at 1.0
        return min(1.0, vulnerability_score)
    
    def calculate_time_since_call_factor(
        self, call_timestamp: str, current_timestamp: Optional[str]
    ) -> float:
        """
        Calculate time urgency factor (0.0-1.0).
        
        Args:
            call_timestamp: ISO format datetime string
            current_timestamp: Current time (ISO format), defaults to now
            
        Returns:
            Time factor (0.0=just called, 1.0=long delay)
        """
        try:
            call_time = datetime.fromisoformat(call_timestamp.replace("Z", "+00:00"))
            if current_timestamp:
                current_time = datetime.fromisoformat(
                    current_timestamp.replace("Z", "+00:00")
                )
            else:
                current_time = datetime.now(timezone.utc)
            
            # Calculate minutes since call
            time_diff = (current_time - call_time).total_seconds() / 60.0
            
            # Normalize: 0-5 min = 0.0, 5-15 min = 0.2, 15-30 min = 0.5, 30-60 min = 0.8, 60+ min = 1.0
            if time_diff < 5:
                return 0.0
            elif time_diff < 15:
                return 0.2
            elif time_diff < 30:
                return 0.5
            elif time_diff < 60:
                return 0.8
            else:
                return 1.0
        except Exception as e:
            logger.warning(
                "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.calculate_time_since_call_factor.error",
                error=str(e),
                call_timestamp=call_timestamp
            )
            return 0.0
    
    def calculate_confidence(
        self, factors: RiskFactors, input_data: RiskScoreInput
    ) -> float:
        """
        Calculate confidence in the risk score based on data completeness.
        
        Args:
            factors: Calculated risk factors
            input_data: Original input data
            
        Returns:
            Confidence score (0.0-1.0)
        """
        confidence_components = []
        
        # Call classification confidence
        confidence_components.append(input_data.call_classification.confidence)
        
        # Fire data availability
        if input_data.nearest_fire:
            confidence_components.append(0.9)
        else:
            confidence_components.append(0.5)  # Lower confidence without fire data
        
        # Weather data availability
        if input_data.weather_data:
            confidence_components.append(0.9)
        else:
            confidence_components.append(0.6)  # Moderate confidence without weather
        
        # Vulnerability data availability
        if input_data.vulnerability_factors:
            confidence_components.append(0.9)
        else:
            confidence_components.append(0.7)  # Moderate confidence with default vulnerability
        
        # Average confidence
        return sum(confidence_components) / len(confidence_components)
    
    
    def calculate_risk_score(self, input_data: RiskScoreInput) -> RiskScoreResult:
        """
        Calculate comprehensive risk score for an emergency call.
        
        Uses hybrid approach: tries Vertex AI if available and configured,
        otherwise falls back to rule-based algorithm.
        
        Args:
            input_data: Risk score input data
            
        Returns:
            Risk score result with factors, score, and recommendation
        """
        logger.debug(
            "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.calculate_risk_score.starting",
            call_id=input_data.call_id
        )
        
        # Try Vertex AI if available
        if self.vertex_ai_scorer and self.vertex_ai_scorer.is_available():
            try:
                logger.debug(
                    "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.calculate_risk_score.using_vertex_ai",
                    call_id=input_data.call_id
                )
                return self.vertex_ai_scorer.calculate_risk_score(input_data)
            except Exception as e:
                logger.warning(
                    "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.calculate_risk_score.vertex_ai_failed",
                    error=str(e),
                    call_id=input_data.call_id,
                    falling_back="rule-based"
                )
                # Fall through to rule-based
        
        # Use rule-based scoring (fallback or primary)
        logger.debug(
            "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer.calculate_risk_score.using_rule_based",
            call_id=input_data.call_id
        )
        return self._calculate_rule_based_score(input_data)
    
    def _calculate_rule_based_score(self, input_data: RiskScoreInput) -> RiskScoreResult:
        """
        Calculate risk score using rule-based algorithm.
        
        Args:
            input_data: Risk score input data
            
        Returns:
            Risk score result with factors, score, and recommendation
        """
        # Calculate individual factors
        call_urgency = self.calculate_call_urgency_factor(
            input_data.call_classification,
            phase=input_data.phase
        )
        fire_proximity = self.calculate_fire_proximity_factor(
            input_data.caller_location, input_data.nearest_fire
        )
        fire_spread_rate = self.calculate_fire_spread_rate_factor(
            input_data.caller_location, input_data.nearest_fire
        )
        weather_risk = self.calculate_weather_risk_factor(input_data.weather_data)
        vulnerability = self.calculate_vulnerability_factor(
            input_data.vulnerability_factors
        )
        time_since_call = self.calculate_time_since_call_factor(
            input_data.call_timestamp, input_data.current_timestamp
        )
        
        # Create factors object
        factors = RiskFactors(
            call_urgency=call_urgency,
            fire_proximity=fire_proximity,
            fire_spread_rate=fire_spread_rate if fire_spread_rate > 0 else None,
            weather_risk=weather_risk if weather_risk > 0 else None,
            vulnerability=vulnerability,
            time_since_call=time_since_call if time_since_call > 0 else None,
        )
        
        # Calculate weighted risk score (0-100)
        weighted_score = (
            call_urgency * self.weights["call_urgency"] * 100
            + fire_proximity * self.weights["fire_proximity"] * 100
            + (fire_spread_rate or 0.0) * self.weights["fire_spread_rate"] * 100
            + (weather_risk or 0.0) * self.weights["weather_risk"] * 100
            + vulnerability * self.weights["vulnerability"] * 100
            + (time_since_call or 0.0) * self.weights["time_since_call"] * 100
        )
        
        # Calculate confidence
        confidence = self.calculate_confidence(factors, input_data)
        
        # Create result with recommendation and priority
        result = create_risk_score_result(
            call_id=input_data.call_id,
            risk_score=weighted_score,
            confidence=confidence,
            factors=factors,
            current_timestamp=input_data.current_timestamp,
        )
        
        logger.info(
            "sentinel_dispatch.ai_ml.risk_scorer.RiskScorer._calculate_rule_based_score.completed",
            call_id=input_data.call_id,
            risk_score=result.risk_score,
            recommendation=result.recommendation,
            priority=result.priority
        )
        
        return result


# Convenience function
def calculate_risk_score(input_data: RiskScoreInput) -> RiskScoreResult:
    """
    Convenience function to calculate risk score using hybrid approach.
    
    Automatically uses Vertex AI if available and configured,
    otherwise falls back to rule-based algorithm.
    
    Args:
        input_data: Risk score input data
        
    Returns:
        Risk score result
    """
    scorer = RiskScorer()
    return scorer.calculate_risk_score(input_data)

