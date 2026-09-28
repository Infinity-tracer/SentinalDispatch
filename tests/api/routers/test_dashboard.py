"""
Tests for dashboard API endpoints.

These tests use an in-memory SQLite database for testing.
"""

import pytest
from fastapi.testclient import TestClient

from sentinel_dispatch.api.main import app
from sentinel_dispatch.api.database import (
    get_db_manager,
    DatabaseManager,
    emergency_call_schema_to_model,
    risk_score_schema_to_model,
    alert_schema_to_model,
)
from sentinel_dispatch.api.schemas import (
    EmergencyCall,
    CallerLocation,
    CallMetadata,
    DashboardRiskScore,
    Alert,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_test_db():
    """Set up in-memory test database before each test."""
    # Use in-memory SQLite for testing
    import sentinel_dispatch.api.database as db_module
    db_module._db_manager = DatabaseManager("sqlite:///:memory:")
    db_module._db_manager.init_db()
    
    yield
    
    # Cleanup after test
    db_module._db_manager.close()
    db_module._db_manager = None


def _add_test_call(call: EmergencyCall, is_enriched: bool = False):
    """Helper to add a test call to the database."""
    db = get_db_manager().get_session()
    try:
        call_model = emergency_call_schema_to_model(call, is_enriched=is_enriched)
        db.merge(call_model)
        db.commit()
    finally:
        db.close()


def _add_test_risk_score(score: DashboardRiskScore):
    """Helper to add a test risk score to the database."""
    db = get_db_manager().get_session()
    try:
        score_model = risk_score_schema_to_model(score)
        db.merge(score_model)
        db.commit()
    finally:
        db.close()


def _add_test_alert(alert: Alert):
    """Helper to add a test alert to the database."""
    db = get_db_manager().get_session()
    try:
        alert_model = alert_schema_to_model(alert)
        db.merge(alert_model)
        db.commit()
    finally:
        db.close()


class TestGetCalls:
    """Test cases for GET /api/calls endpoint."""
    
    def setup_method(self):
        """Set up test data before each test."""
        # Add test calls
        test_calls = [
            EmergencyCall(
                callId="CALL-001",
                timestamp="2024-08-08T14:23:45Z",
                callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
                transcript="I need help! The fire is getting close!",
                metadata=CallMetadata(
                    phoneNumber="+1-808-555-0123",
                    callDuration=127,
                    callerAge=65,
                    knownConditions=["asthma"]
                ),
            ),
            EmergencyCall(
                callId="CALL-002",
                timestamp="2024-08-08T14:25:00Z",
                callerLocation=CallerLocation(lat=20.8800, lon=-156.6800),
                transcript="Smoke is everywhere, I can't breathe!",
                metadata=None,
            ),
            EmergencyCall(
                callId="CALL-003",
                timestamp="2024-08-08T14:20:00Z",
                callerLocation=CallerLocation(lat=20.8750, lon=-156.6850),
                transcript="My neighbor is trapped, please help!",
                metadata=CallMetadata(
                    phoneNumber="+1-808-555-0456",
                    callDuration=89,
                ),
            ),
        ]
        
        for call in test_calls:
            _add_test_call(call, is_enriched=False)
    
    def test_get_calls_success(self):
        """Test successful retrieval of all calls."""
        response = client.get("/api/calls")
        
        assert response.status_code == 200
        data = response.json()
        assert "calls" in data
        assert "total" in data
        assert data["total"] == 3
        assert len(data["calls"]) == 3
        # Should be sorted by timestamp (most recent first)
        assert data["calls"][0]["callId"] == "CALL-002"  # Most recent
        assert data["calls"][1]["callId"] == "CALL-001"
        assert data["calls"][2]["callId"] == "CALL-003"  # Oldest
    
    def test_get_calls_with_limit(self):
        """Test getting calls with limit parameter."""
        response = client.get("/api/calls?limit=2")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["calls"]) == 2
        assert data["calls"][0]["callId"] == "CALL-002"
        assert data["calls"][1]["callId"] == "CALL-001"
    
    def test_get_calls_with_offset(self):
        """Test getting calls with offset parameter."""
        response = client.get("/api/calls?offset=1&limit=2")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["calls"]) == 2
        assert data["calls"][0]["callId"] == "CALL-001"
        assert data["calls"][1]["callId"] == "CALL-003"
    
    
    def test_get_calls_invalid_limit(self):
        """Test getting calls with invalid limit (too high)."""
        response = client.get("/api/calls?limit=2000")
        
        # Should return 422 validation error
        assert response.status_code == 422
    
    def test_get_calls_invalid_offset(self):
        """Test getting calls with invalid offset (negative)."""
        response = client.get("/api/calls?offset=-1")
        
        # Should return 422 validation error
        assert response.status_code == 422


class TestGetCall:
    """Test cases for GET /api/calls/{call_id} endpoint."""
    
    def setup_method(self):
        """Set up test data before each test."""
        test_call = EmergencyCall(
            callId="CALL-001",
            timestamp="2024-08-08T14:23:45Z",
            callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
            transcript="I need help! The fire is getting close!",
            metadata=CallMetadata(
                phoneNumber="+1-808-555-0123",
                callDuration=127,
                callerAge=65,
                knownConditions=["asthma"]
            ),
        )
        _add_test_call(test_call)
    
    def test_get_call_success(self):
        """Test successful retrieval of a specific call."""
        response = client.get("/api/calls/CALL-001")
        
        assert response.status_code == 200
        data = response.json()
        assert data["callId"] == "CALL-001"
        assert data["transcript"] == "I need help! The fire is getting close!"
        assert data["callerLocation"]["lat"] == 20.8783
        assert data["callerLocation"]["lon"] == -156.6825
        assert data["metadata"]["phoneNumber"] == "+1-808-555-0123"
        assert data["metadata"]["callerAge"] == 65
        assert data["metadata"]["knownConditions"] == ["asthma"]
    
    def test_get_call_not_found(self):
        """Test getting a call that doesn't exist."""
        response = client.get("/api/calls/CALL-999")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()


class TestGetRiskScores:
    """Test cases for GET /api/risk-scores endpoint."""
    
    def setup_method(self):
        """Set up test data before each test."""
        # Add enriched calls (required for risk scores to be returned)
        enriched_calls = [
            EmergencyCall(
                callId="CALL-001",
                timestamp="2024-08-08T14:23:45Z",
                callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
                transcript="I need help!",
                metadata=None,
            ),
            EmergencyCall(
                callId="CALL-002",
                timestamp="2024-08-08T14:25:00Z",
                callerLocation=CallerLocation(lat=20.8800, lon=-156.6800),
                transcript="Smoke is everywhere!",
                metadata=None,
            ),
            EmergencyCall(
                callId="CALL-003",
                timestamp="2024-08-08T14:20:00Z",
                callerLocation=CallerLocation(lat=20.8750, lon=-156.6850),
                transcript="My neighbor is trapped!",
                metadata=None,
            ),
        ]
        
        for call in enriched_calls:
            _add_test_call(call, is_enriched=True)
        
        test_scores = [
            DashboardRiskScore(
                callId="CALL-001",
                timestamp="2024-08-08T14:23:50Z",
                riskScore=85.0,
                confidence=0.95,
                factors={
                    "callUrgency": 0.9,
                    "fireProximity": 0.8,
                    "fireSpreadRate": 0.7,
                    "weatherRisk": 0.6,
                    "vulnerability": 0.9,
                },
                recommendation="IMMEDIATE_DISPATCH",
                priority=5,
            ),
            DashboardRiskScore(
                callId="CALL-002",
                timestamp="2024-08-08T14:25:05Z",
                riskScore=65.0,
                confidence=0.85,
                factors={
                    "callUrgency": 0.7,
                    "fireProximity": 0.6,
                    "fireSpreadRate": 0.5,
                    "weatherRisk": 0.4,
                    "vulnerability": 0.5,
                },
                recommendation="HIGH_PRIORITY",
                priority=4,
            ),
            DashboardRiskScore(
                callId="CALL-003",
                timestamp="2024-08-08T14:20:05Z",
                riskScore=45.0,
                confidence=0.75,
                factors={
                    "callUrgency": 0.5,
                    "fireProximity": 0.4,
                    "weatherRisk": 0.3,
                    "vulnerability": 0.3,
                },
                recommendation="MODERATE_PRIORITY",
                priority=3,
            ),
        ]
        
        for score in test_scores:
            _add_test_risk_score(score)
    
    def test_get_risk_scores_success(self):
        """Test successful retrieval of all risk scores."""
        response = client.get("/api/risk-scores")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 3
        # Should be sorted by priority (highest first), then risk score
        assert data[0]["callId"] == "CALL-001"  # Priority 5, score 85.0
        assert data[0]["priority"] == 5
        assert data[0]["riskScore"] == 85.0
        assert data[1]["callId"] == "CALL-002"  # Priority 4, score 65.0
        assert data[2]["callId"] == "CALL-003"  # Priority 3, score 45.0
    
    def test_get_risk_scores_with_call_id(self):
        """Test getting risk score for a specific call."""
        response = client.get("/api/risk-scores?call_id=CALL-001")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["callId"] == "CALL-001"
        assert data[0]["riskScore"] == 85.0
        assert data[0]["priority"] == 5
    
    def test_get_risk_scores_with_call_id_not_found(self):
        """Test getting risk score for a call that doesn't exist."""
        response = client.get("/api/risk-scores?call_id=CALL-999")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
    
    def test_get_risk_scores_excludes_unprocessed_calls(self):
        """Test that risk scores are only returned for fully processed (enriched) calls."""
        # Add a risk score for a call that hasn't been enriched
        unprocessed_score = DashboardRiskScore(
            callId="CALL-004",
            timestamp="2024-08-08T14:30:00Z",
            riskScore=50.0,
            confidence=0.8,
            factors={"callUrgency": 0.5},
            recommendation="MODERATE_PRIORITY",
            priority=3,
        )
        _add_test_risk_score(unprocessed_score)
        # Note: CALL-004 is NOT added as enriched
        
        response = client.get("/api/risk-scores")
        
        assert response.status_code == 200
        data = response.json()
        # Should only return scores for enriched calls (CALL-001, CALL-002, CALL-003)
        assert len(data) == 3
        call_ids = [score["callId"] for score in data]
        assert "CALL-004" not in call_ids
    
    def test_get_risk_scores_with_limit(self):
        """Test getting risk scores with limit parameter."""
        response = client.get("/api/risk-scores?limit=2")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["callId"] == "CALL-001"
        assert data[1]["callId"] == "CALL-002"
    


class TestGetPrioritizedCases:
    """Test cases for GET /api/prioritized-cases endpoint."""
    
    def setup_method(self):
        """Set up test data before each test."""
        # Add enriched calls (required for prioritized cases)
        calls = [
            EmergencyCall(
                callId="CALL-001",
                timestamp="2024-08-08T14:23:45Z",
                callerLocation=CallerLocation(lat=20.8783, lon=-156.6825),
                transcript="I need help! The fire is getting close!",
                metadata=None,
            ),
            EmergencyCall(
                callId="CALL-002",
                timestamp="2024-08-08T14:25:00Z",
                callerLocation=CallerLocation(lat=20.8800, lon=-156.6800),
                transcript="Smoke is everywhere!",
                metadata=None,
            ),
            EmergencyCall(
                callId="CALL-003",
                timestamp="2024-08-08T14:20:00Z",
                callerLocation=CallerLocation(lat=20.8750, lon=-156.6850),
                transcript="My neighbor is trapped!",
                metadata=None,
            ),
        ]
        
        for call in calls:
            _add_test_call(call, is_enriched=True)
        
        # Add risk scores
        scores = [
            DashboardRiskScore(
                callId="CALL-001",
                timestamp="2024-08-08T14:23:50Z",
                riskScore=85.0,
                confidence=0.95,
                factors={"callUrgency": 0.9, "fireProximity": 0.8},
                recommendation="IMMEDIATE_DISPATCH",
                priority=5,
            ),
            DashboardRiskScore(
                callId="CALL-002",
                timestamp="2024-08-08T14:25:05Z",
                riskScore=65.0,
                confidence=0.85,
                factors={"callUrgency": 0.7, "fireProximity": 0.6},
                recommendation="HIGH_PRIORITY",
                priority=4,
            ),
            DashboardRiskScore(
                callId="CALL-003",
                timestamp="2024-08-08T14:20:05Z",
                riskScore=45.0,
                confidence=0.75,
                factors={"callUrgency": 0.5, "fireProximity": 0.4},
                recommendation="MODERATE_PRIORITY",
                priority=3,
            ),
        ]
        
        for score in scores:
            _add_test_risk_score(score)
        
        # Add alerts for CALL-001
        alerts = [
            Alert(
                alertId="ALERT-001",
                timestamp="2024-08-08T14:24:00Z",
                severity="critical",
                message="High risk case requiring immediate attention",
                alert_type="risk_threshold",
                callId="CALL-001",
                acknowledged=False,
            ),
        ]
        
        for alert in alerts:
            _add_test_alert(alert)
    
    def test_get_prioritized_cases_success(self):
        """Test successful retrieval of prioritized cases."""
        response = client.get("/api/prioritized-cases")
        
        assert response.status_code == 200
        data = response.json()
        assert "cases" in data
        assert "total" in data
        assert data["total"] == 3
        assert len(data["cases"]) == 3
        
        # Should be sorted by priority (highest first), then risk score
        assert data["cases"][0]["callId"] == "CALL-001"
        assert data["cases"][0]["priority"] == 5
        assert data["cases"][0]["riskScore"]["riskScore"] == 85.0
        assert len(data["cases"][0]["alerts"]) == 1
        assert data["cases"][0]["alerts"][0]["alertId"] == "ALERT-001"
        
        assert data["cases"][1]["callId"] == "CALL-002"
        assert data["cases"][1]["priority"] == 4
        assert data["cases"][1]["alerts"] is None
        
        assert data["cases"][2]["callId"] == "CALL-003"
        assert data["cases"][2]["priority"] == 3
    
    def test_get_prioritized_cases_with_min_priority(self):
        """Test getting prioritized cases with minimum priority filter."""
        response = client.get("/api/prioritized-cases?min_priority=4")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2  # Only CALL-001 and CALL-002
        assert len(data["cases"]) == 2
        assert data["cases"][0]["callId"] == "CALL-001"
        assert data["cases"][1]["callId"] == "CALL-002"
        # All should have priority >= 4
        assert all(case["priority"] >= 4 for case in data["cases"])
    
    def test_get_prioritized_cases_with_limit(self):
        """Test getting prioritized cases with limit parameter."""
        response = client.get("/api/prioritized-cases?limit=2")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 3
        assert len(data["cases"]) == 2
        assert data["cases"][0]["callId"] == "CALL-001"
        assert data["cases"][1]["callId"] == "CALL-002"
    
    def test_get_prioritized_cases_excludes_calls_without_scores(self):
        """Test that calls without risk scores are excluded."""
        # Add an enriched call without a risk score
        call_without_score = EmergencyCall(
            callId="CALL-004",
            timestamp="2024-08-08T14:30:00Z",
            callerLocation=CallerLocation(lat=20.8700, lon=-156.6900),
            transcript="Test call",
            metadata=None,
        )
        _add_test_call(call_without_score, is_enriched=True)
        
        response = client.get("/api/prioritized-cases")
        
        assert response.status_code == 200
        data = response.json()
        # Should still only have 3 cases (CALL-004 excluded because no risk score)
        assert data["total"] == 3
        call_ids = [case["callId"] for case in data["cases"]]
        assert "CALL-004" not in call_ids
    
    def test_get_prioritized_cases_excludes_unprocessed_calls(self):
        """Test that calls that haven't been fully processed (enriched) are excluded."""
        # Add a call and risk score, but don't mark as enriched
        unprocessed_call = EmergencyCall(
            callId="CALL-005",
            timestamp="2024-08-08T14:35:00Z",
            callerLocation=CallerLocation(lat=20.8700, lon=-156.6900),
            transcript="Unprocessed call",
            metadata=None,
        )
        _add_test_call(unprocessed_call, is_enriched=False)  # Not enriched
        
        unprocessed_score = DashboardRiskScore(
            callId="CALL-005",
            timestamp="2024-08-08T14:35:05Z",
            riskScore=60.0,
            confidence=0.85,
            factors={"callUrgency": 0.6},
            recommendation="HIGH_PRIORITY",
            priority=4,
        )
        _add_test_risk_score(unprocessed_score)
        
        response = client.get("/api/prioritized-cases")
        
        assert response.status_code == 200
        data = response.json()
        # Should still only have 3 cases (CALL-005 excluded because not enriched)
        assert data["total"] == 3
        call_ids = [case["callId"] for case in data["cases"]]
        assert "CALL-005" not in call_ids
    


class TestGetAlerts:
    """Test cases for GET /api/alerts endpoint."""
    
    def setup_method(self):
        """Set up test data before each test."""
        test_alerts = [
            Alert(
                alertId="ALERT-001",
                timestamp="2024-08-08T14:24:00Z",
                severity="critical",
                message="High risk case requiring immediate attention",
                alert_type="risk_threshold",
                callId="CALL-001",
                acknowledged=False,
            ),
            Alert(
                alertId="ALERT-002",
                timestamp="2024-08-08T14:25:00Z",
                severity="high",
                message="Fire approaching caller location",
                alert_type="fire_proximity",
                callId="CALL-002",
                acknowledged=False,
            ),
            Alert(
                alertId="ALERT-003",
                timestamp="2024-08-08T14:26:00Z",
                severity="medium",
                message="Moderate risk escalation",
                alert_type="urgency_escalation",
                callId="CALL-003",
                acknowledged=True,
            ),
            Alert(
                alertId="ALERT-004",
                timestamp="2024-08-08T14:27:00Z",
                severity="low",
                message="Low priority alert",
                alert_type="weather_hazard",
                callId="CALL-004",
                acknowledged=False,
            ),
        ]
        
        for alert in test_alerts:
            _add_test_alert(alert)
    
    def test_get_alerts_success(self):
        """Test successful retrieval of all alerts."""
        response = client.get("/api/alerts")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 4
        # Should be sorted by severity (critical first), then timestamp
        assert data[0]["alertId"] == "ALERT-001"  # critical
        assert data[0]["severity"] == "critical"
        assert data[1]["alertId"] == "ALERT-002"  # high
        assert data[2]["alertId"] == "ALERT-003"  # medium
        assert data[3]["alertId"] == "ALERT-004"  # low
    
    def test_get_alerts_with_call_id_filter(self):
        """Test getting alerts filtered by call ID."""
        response = client.get("/api/alerts?call_id=CALL-001")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["alertId"] == "ALERT-001"
        assert data[0]["callId"] == "CALL-001"
    
    def test_get_alerts_with_severity_filter(self):
        """Test getting alerts filtered by severity."""
        response = client.get("/api/alerts?severity=critical")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["severity"] == "critical"
        assert data[0]["alertId"] == "ALERT-001"
    
    def test_get_alerts_with_acknowledged_filter(self):
        """Test getting alerts filtered by acknowledged status."""
        response = client.get("/api/alerts?acknowledged=false")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 3  # ALERT-003 is acknowledged
        assert all(alert["acknowledged"] is False for alert in data)
        
        # Test acknowledged=true
        response = client.get("/api/alerts?acknowledged=true")
        data = response.json()
        assert len(data) == 1
        assert data[0]["alertId"] == "ALERT-003"
        assert data[0]["acknowledged"] is True
    
    def test_get_alerts_with_multiple_filters(self):
        """Test getting alerts with multiple filters."""
        response = client.get("/api/alerts?severity=high&acknowledged=false")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["severity"] == "high"
        assert data[0]["acknowledged"] is False
        assert data[0]["alertId"] == "ALERT-002"
    
    def test_get_alerts_with_limit(self):
        """Test getting alerts with limit parameter."""
        response = client.get("/api/alerts?limit=2")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["alertId"] == "ALERT-001"
        assert data[1]["alertId"] == "ALERT-002"


class TestGetCallsEmpty:
    """Test cases for GET /api/calls endpoint with empty database."""
    
    # No setup_method - database starts empty
    
    def test_get_calls_empty(self):
        """Test getting calls when database is empty."""
        response = client.get("/api/calls")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert len(data["calls"]) == 0


class TestGetRiskScoresEmpty:
    """Test cases for GET /api/risk-scores endpoint with empty database."""
    
    # No setup_method - database starts empty
    
    def test_get_risk_scores_empty(self):
        """Test getting risk scores when database is empty."""
        response = client.get("/api/risk-scores")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0


class TestGetPrioritizedCasesEmpty:
    """Test cases for GET /api/prioritized-cases endpoint with empty database."""
    
    # No setup_method - database starts empty
    
    def test_get_prioritized_cases_empty(self):
        """Test getting prioritized cases when database is empty."""
        response = client.get("/api/prioritized-cases")
        
        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert len(data["cases"]) == 0


class TestGetAlertsEmpty:
    """Test cases for GET /api/alerts endpoint with empty database."""
    
    # No setup_method - database starts empty
    
    def test_get_alerts_empty(self):
        """Test getting alerts when database is empty."""
        response = client.get("/api/alerts")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
    
