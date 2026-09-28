"""
WebSocket connection manager for real-time updates.

Manages multiple WebSocket connections and broadcasts messages to all connected clients.
"""

import asyncio
from datetime import datetime, timezone
from typing import Set, Optional
from enum import Enum

from fastapi import WebSocket
from sentinel_dispatch.utils.logging_config import get_logger

logger = get_logger(__name__)


class WebSocketMessageType(str, Enum):
    """WebSocket message types."""
    
    RISK_SCORE_UPDATE = "risk_score_update"
    NEW_CALL = "new_call"
    ALERT = "alert"
    FIRE_UPDATE = "fire_update"
    WEATHER_UPDATE = "weather_update"
    PING = "ping"
    PONG = "pong"


class WebSocketConnectionManager:
    """Manages WebSocket connections and broadcasts messages."""
    
    def __init__(self):
        """Initialize the connection manager."""
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        logger.info(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.__init__.initialized",
        )
    
    async def connect(self, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.connect.connected",
            total_connections=len(self.active_connections),
        )
    
    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        async with self._lock:
            self.active_connections.discard(websocket)
        logger.info(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.disconnect.disconnected",
            total_connections=len(self.active_connections),
        )
    
    async def send_personal_message(self, message: dict, websocket: WebSocket) -> None:
        """Send a message to a specific WebSocket connection."""
        try:
            await websocket.send_json(message)
        except Exception as e:
            logger.error(
                "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.send_personal_message.error",
                error=str(e),
                exc_info=True,
            )
            # Remove connection if it's broken
            await self.disconnect(websocket)
    
    async def broadcast(self, message: dict) -> None:
        """Broadcast a message to all connected WebSocket clients."""
        if not self.active_connections:
            return
        
        # Create a copy of connections to avoid modification during iteration
        async with self._lock:
            connections = list(self.active_connections)
        
        # Send to all connections
        disconnected = []
        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(
                    "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast.send_error",
                    error=str(e),
                )
                disconnected.append(connection)
        
        # Remove disconnected connections
        if disconnected:
            async with self._lock:
                for conn in disconnected:
                    self.active_connections.discard(conn)
            logger.info(
                "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast.removed_disconnected",
                count=len(disconnected),
            )
    
    async def broadcast_risk_score_update(self, risk_score: dict) -> None:
        """Broadcast a risk score update."""
        message = {
            "type": WebSocketMessageType.RISK_SCORE_UPDATE,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": risk_score,
        }
        await self.broadcast(message)
        logger.debug(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast_risk_score_update.sent",
            call_id=risk_score.get("callId"),
        )
    
    async def broadcast_new_call(self, call: dict) -> None:
        """Broadcast a new emergency call."""
        message = {
            "type": WebSocketMessageType.NEW_CALL,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": call,
        }
        await self.broadcast(message)
        logger.debug(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast_new_call.sent",
            call_id=call.get("callId"),
        )
    
    async def broadcast_alert(self, alert: dict) -> None:
        """Broadcast an alert."""
        message = {
            "type": WebSocketMessageType.ALERT,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": alert,
        }
        await self.broadcast(message)
        logger.debug(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast_alert.sent",
            alert_id=alert.get("alertId"),
        )
    
    async def broadcast_fire_update(self, fire_data: dict) -> None:
        """Broadcast a fire data update."""
        message = {
            "type": WebSocketMessageType.FIRE_UPDATE,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": fire_data,
        }
        await self.broadcast(message)
        logger.debug(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast_fire_update.sent",
            fire_id=fire_data.get("fireId"),
        )
    
    async def broadcast_weather_update(self, weather_data: dict) -> None:
        """Broadcast a weather data update."""
        message = {
            "type": WebSocketMessageType.WEATHER_UPDATE,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": weather_data,
        }
        await self.broadcast(message)
        logger.debug(
            "sentinel_dispatch.api.websocket_manager.WebSocketConnectionManager.broadcast_weather_update.sent",
        )
    
    def get_connection_count(self) -> int:
        """Get the number of active connections."""
        return len(self.active_connections)


# Global connection manager instance
_connection_manager: Optional[WebSocketConnectionManager] = None


def get_connection_manager() -> WebSocketConnectionManager:
    """Get the global WebSocket connection manager."""
    global _connection_manager
    if _connection_manager is None:
        _connection_manager = WebSocketConnectionManager()
    return _connection_manager

