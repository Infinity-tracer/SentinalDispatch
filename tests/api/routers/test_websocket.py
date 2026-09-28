"""
Tests for WebSocket router endpoints.
"""

import json
from unittest.mock import Mock, AsyncMock, patch

import pytest
from fastapi import WebSocketDisconnect

from sentinel_dispatch.api.routers import websocket
from sentinel_dispatch.api.schemas import Alert
from sentinel_dispatch.api.websocket_manager import WebSocketMessageType
from sentinel_dispatch.api.database import (
    get_db_manager,
    DatabaseManager,
    AlertModel,
    alert_schema_to_model,
)


@pytest.fixture(autouse=True)
def setup_test_db():
    """Set up in-memory test database before each test."""
    import sentinel_dispatch.api.database as db_module
    db_module._db_manager = DatabaseManager("sqlite:///:memory:")
    db_module._db_manager.init_db()
    
    yield
    
    # Cleanup after test
    db_module._db_manager.close()
    db_module._db_manager = None


class TestWebSocketEndpoint:
    """Test cases for WebSocket endpoint."""
    
    # No setup_method - database starts empty via fixture
    
    @pytest.mark.asyncio
    async def test_websocket_connect(self):
        """Test WebSocket connection."""
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        # Raise WebSocketDisconnect immediately to exit the loop
        mock_websocket.receive_text = AsyncMock(side_effect=WebSocketDisconnect())
        
        # Mock connection manager
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                # The endpoint should catch WebSocketDisconnect and exit gracefully
                await websocket.websocket_endpoint(mock_websocket)
        
        mock_manager.connect.assert_called_once_with(mock_websocket)
        mock_manager.disconnect.assert_called_once_with(mock_websocket)
    
    @pytest.mark.asyncio
    async def test_websocket_ping_pong(self):
        """Test ping/pong message handling."""
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        # First message is ping, second causes disconnect
        mock_websocket.receive_text = AsyncMock(side_effect=[
            json.dumps({"type": "ping", "timestamp": "2024-08-08T14:23:45Z"}),
            WebSocketDisconnect(),
        ])
        
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                try:
                    await websocket.websocket_endpoint(mock_websocket)
                except WebSocketDisconnect:
                    pass
        
        # Verify pong response
        mock_websocket.send_json.assert_called_once()
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == WebSocketMessageType.PONG
        assert call_args["timestamp"] == "2024-08-08T14:23:45Z"
    
    @pytest.mark.asyncio
    async def test_websocket_acknowledge_alert(self):
        """Test alert acknowledgment via WebSocket."""
        # Setup alert in database
        test_alert = Alert(
            alertId="ALERT-001",
            timestamp="2024-08-08T14:24:00Z",
            severity="critical",
            message="High risk case",
            alert_type="risk_threshold",
            callId="CALL-001",
            acknowledged=False,
        )
        db = get_db_manager().get_session()
        try:
            alert_model = alert_schema_to_model(test_alert)
            db.merge(alert_model)
            db.commit()
        finally:
            db.close()
        
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        # First message is acknowledge_alert, second causes disconnect
        mock_websocket.receive_text = AsyncMock(side_effect=[
            json.dumps({"type": "acknowledge_alert", "alert_id": "ALERT-001"}),
            WebSocketDisconnect(),
        ])
        
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.broadcast_alert = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                try:
                    await websocket.websocket_endpoint(mock_websocket)
                except WebSocketDisconnect:
                    pass
        
        # Verify alert was acknowledged in database
        db = get_db_manager().get_session()
        try:
            alert_model = db.query(AlertModel).filter_by(alert_id="ALERT-001").first()
            assert alert_model is not None
            assert alert_model.acknowledged is True
        finally:
            db.close()
        
        # Verify alert was broadcast
        mock_manager.broadcast_alert.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_websocket_acknowledge_alert_not_found(self):
        """Test acknowledging an alert that doesn't exist."""
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        
        # First message is acknowledge_alert for non-existent alert, second causes disconnect
        mock_websocket.receive_text = AsyncMock(side_effect=[
            json.dumps({"type": "acknowledge_alert", "alert_id": "ALERT-999"}),
            WebSocketDisconnect(),
        ])
        
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                try:
                    await websocket.websocket_endpoint(mock_websocket)
                except WebSocketDisconnect:
                    pass
        
        # Should not raise an error, just log warning
        mock_manager.disconnect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_websocket_invalid_json(self):
        """Test handling invalid JSON messages."""
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        
        # First message is invalid JSON, second causes disconnect
        mock_websocket.receive_text = AsyncMock(side_effect=[
            "not valid json",
            WebSocketDisconnect(),
        ])
        
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                try:
                    await websocket.websocket_endpoint(mock_websocket)
                except WebSocketDisconnect:
                    pass
        
        # Should not raise an error, just log warning and continue
        mock_manager.disconnect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_websocket_unknown_message_type(self):
        """Test handling unknown message types."""
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        
        # First message is unknown type, second causes disconnect
        mock_websocket.receive_text = AsyncMock(side_effect=[
            json.dumps({"type": "unknown_type", "data": "test"}),
            WebSocketDisconnect(),
        ])
        
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                try:
                    await websocket.websocket_endpoint(mock_websocket)
                except WebSocketDisconnect:
                    pass
        
        # Should not raise an error, just log warning
        mock_manager.disconnect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_websocket_receive_error(self):
        """Test handling receive errors."""
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        # Raise exception once, then WebSocketDisconnect to exit loop
        mock_websocket.receive_text = AsyncMock(side_effect=[
            Exception("Receive error"),
            WebSocketDisconnect(),
        ])
        
        mock_manager = AsyncMock()
        mock_manager.connect = AsyncMock()
        mock_manager.disconnect = AsyncMock()
        mock_manager.get_connection_count = Mock(return_value=1)
        
        with patch("sentinel_dispatch.api.routers.websocket.get_connection_manager", return_value=mock_manager):
            with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
                # Should handle error gracefully, log it, continue, then disconnect
                await websocket.websocket_endpoint(mock_websocket)
        
        mock_manager.disconnect.assert_called_once()
        # Verify receive_text was called (once for error, once for disconnect)
        assert mock_websocket.receive_text.call_count >= 1


class TestWebSocketStatus:
    """Test cases for WebSocket status endpoint."""
    
    @pytest.mark.asyncio
    async def test_websocket_status(self):
        """Test WebSocket status endpoint."""
        mock_manager = Mock()
        mock_manager.get_connection_count = Mock(return_value=3)
        
        with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
            result = await websocket.websocket_status()
        
        assert result["active_connections"] == 3
        assert result["status"] == "active"
    
    @pytest.mark.asyncio
    async def test_websocket_status_no_connections(self):
        """Test WebSocket status with no connections."""
        mock_manager = Mock()
        mock_manager.get_connection_count = Mock(return_value=0)
        
        with patch("sentinel_dispatch.api.routers.websocket.connection_manager", mock_manager):
            result = await websocket.websocket_status()
        
        assert result["active_connections"] == 0
        assert result["status"] == "active"

