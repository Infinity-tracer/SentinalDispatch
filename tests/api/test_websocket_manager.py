"""
Tests for WebSocket connection manager.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sentinel_dispatch.api.websocket_manager import (
    WebSocketConnectionManager,
    WebSocketMessageType,
    get_connection_manager,
)


class TestWebSocketConnectionManager:
    """Test cases for WebSocketConnectionManager."""
    
    def test_initialization(self):
        """Test connection manager initialization."""
        manager = WebSocketConnectionManager()
        
        assert manager.active_connections == set()
        assert manager.get_connection_count() == 0
    
    @pytest.mark.asyncio
    async def test_connect(self):
        """Test connecting a WebSocket client."""
        manager = WebSocketConnectionManager()
        mock_websocket = MagicMock()
        mock_websocket.accept = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        mock_websocket.accept.assert_called_once()
        assert mock_websocket in manager.active_connections
        assert manager.get_connection_count() == 1
    
    @pytest.mark.asyncio
    async def test_disconnect(self):
        """Test disconnecting a WebSocket client."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        
        await manager.connect(mock_websocket)
        assert manager.get_connection_count() == 1
        
        await manager.disconnect(mock_websocket)
        
        assert mock_websocket not in manager.active_connections
        assert manager.get_connection_count() == 0
    
    @pytest.mark.asyncio
    async def test_send_personal_message_success(self):
        """Test sending a message to a specific client."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        message = {"type": "test", "data": "test message"}
        await manager.send_personal_message(message, mock_websocket)
        
        mock_websocket.send_json.assert_called_once_with(message)
    
    @pytest.mark.asyncio
    async def test_send_personal_message_error(self):
        """Test sending a message when connection fails."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock(side_effect=Exception("Connection error"))
        
        await manager.connect(mock_websocket)
        assert manager.get_connection_count() == 1
        
        message = {"type": "test", "data": "test message"}
        await manager.send_personal_message(message, mock_websocket)
        
        # Connection should be removed on error
        assert mock_websocket not in manager.active_connections
        assert manager.get_connection_count() == 0
    
    @pytest.mark.asyncio
    async def test_broadcast_single_client(self):
        """Test broadcasting to a single client."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        message = {"type": "test", "data": "broadcast message"}
        await manager.broadcast(message)
        
        mock_websocket.send_json.assert_called_once_with(message)
    
    @pytest.mark.asyncio
    async def test_broadcast_multiple_clients(self):
        """Test broadcasting to multiple clients."""
        manager = WebSocketConnectionManager()
        
        mock_websocket1 = AsyncMock()
        mock_websocket1.accept = AsyncMock()
        mock_websocket1.send_json = AsyncMock()
        
        mock_websocket2 = AsyncMock()
        mock_websocket2.accept = AsyncMock()
        mock_websocket2.send_json = AsyncMock()
        
        await manager.connect(mock_websocket1)
        await manager.connect(mock_websocket2)
        
        message = {"type": "test", "data": "broadcast message"}
        await manager.broadcast(message)
        
        mock_websocket1.send_json.assert_called_once_with(message)
        mock_websocket2.send_json.assert_called_once_with(message)
    
    @pytest.mark.asyncio
    async def test_broadcast_with_disconnected_client(self):
        """Test broadcasting when one client is disconnected."""
        manager = WebSocketConnectionManager()
        
        mock_websocket1 = AsyncMock()
        mock_websocket1.accept = AsyncMock()
        mock_websocket1.send_json = AsyncMock()
        
        mock_websocket2 = AsyncMock()
        mock_websocket2.accept = AsyncMock()
        mock_websocket2.send_json = AsyncMock(side_effect=Exception("Connection error"))
        
        await manager.connect(mock_websocket1)
        await manager.connect(mock_websocket2)
        
        message = {"type": "test", "data": "broadcast message"}
        await manager.broadcast(message)
        
        # Both should be called
        mock_websocket1.send_json.assert_called_once_with(message)
        mock_websocket2.send_json.assert_called_once_with(message)
        
        # Disconnected client should be removed
        assert mock_websocket2 not in manager.active_connections
        assert mock_websocket1 in manager.active_connections
    
    @pytest.mark.asyncio
    async def test_broadcast_no_connections(self):
        """Test broadcasting when no clients are connected."""
        manager = WebSocketConnectionManager()
        
        message = {"type": "test", "data": "broadcast message"}
        # Should not raise an error
        await manager.broadcast(message)
    
    @pytest.mark.asyncio
    async def test_broadcast_risk_score_update(self):
        """Test broadcasting a risk score update."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        risk_score = {
            "callId": "CALL-001",
            "riskScore": 85.0,
            "priority": 5,
        }
        await manager.broadcast_risk_score_update(risk_score)
        
        # Verify message structure
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == WebSocketMessageType.RISK_SCORE_UPDATE
        assert call_args["data"] == risk_score
        assert "timestamp" in call_args
    
    @pytest.mark.asyncio
    async def test_broadcast_new_call(self):
        """Test broadcasting a new call."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        call_data = {
            "callId": "CALL-001",
            "transcript": "I need help!",
        }
        await manager.broadcast_new_call(call_data)
        
        # Verify message structure
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == WebSocketMessageType.NEW_CALL
        assert call_args["data"] == call_data
        assert "timestamp" in call_args
    
    @pytest.mark.asyncio
    async def test_broadcast_alert(self):
        """Test broadcasting an alert."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        alert_data = {
            "alertId": "ALERT-001",
            "severity": "critical",
            "message": "High risk case",
        }
        await manager.broadcast_alert(alert_data)
        
        # Verify message structure
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == WebSocketMessageType.ALERT
        assert call_args["data"] == alert_data
        assert "timestamp" in call_args
    
    @pytest.mark.asyncio
    async def test_broadcast_fire_update(self):
        """Test broadcasting a fire update."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        fire_data = {
            "fireId": "FIRE-001",
            "location": {"lat": 20.8783, "lon": -156.6825},
        }
        await manager.broadcast_fire_update(fire_data)
        
        # Verify message structure
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == WebSocketMessageType.FIRE_UPDATE
        assert call_args["data"] == fire_data
        assert "timestamp" in call_args
    
    @pytest.mark.asyncio
    async def test_broadcast_weather_update(self):
        """Test broadcasting a weather update."""
        manager = WebSocketConnectionManager()
        mock_websocket = AsyncMock()
        mock_websocket.accept = AsyncMock()
        mock_websocket.send_json = AsyncMock()
        
        await manager.connect(mock_websocket)
        
        weather_data = {
            "temperature": 85.0,
            "windSpeed": 15.0,
        }
        await manager.broadcast_weather_update(weather_data)
        
        # Verify message structure
        call_args = mock_websocket.send_json.call_args[0][0]
        assert call_args["type"] == WebSocketMessageType.WEATHER_UPDATE
        assert call_args["data"] == weather_data
        assert "timestamp" in call_args
    
    def test_get_connection_count(self):
        """Test getting connection count."""
        manager = WebSocketConnectionManager()
        assert manager.get_connection_count() == 0


class TestGetConnectionManager:
    """Test cases for get_connection_manager function."""
    
    def test_get_connection_manager_singleton(self):
        """Test that get_connection_manager returns a singleton."""
        manager1 = get_connection_manager()
        manager2 = get_connection_manager()
        
        assert manager1 is manager2
        assert isinstance(manager1, WebSocketConnectionManager)

