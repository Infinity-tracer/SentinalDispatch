"""
WebSocket router for real-time updates.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from sentinel_dispatch.api.websocket_manager import get_connection_manager, WebSocketMessageType
from sentinel_dispatch.utils.logging_config import get_logger
from sqlalchemy.exc import OperationalError
import time
import random

if TYPE_CHECKING:
    from sentinel_dispatch.api.database import AlertModel

logger = get_logger(__name__)


def _update_alert_with_retry(db, alert_model: 'AlertModel', updates: dict, max_retries: int = 3) -> None:
    """
    Update alert model in database with retry logic for SQLite lock errors.
    
    Args:
        db: Database session
        alert_model: AlertModel instance to update
        updates: Dictionary of field updates
        max_retries: Maximum number of retry attempts
    """
    from datetime import datetime as dt
    
    delay = 0.1  # Initial delay in seconds
    max_delay = 1.0
    
    for attempt in range(max_retries + 1):
        try:
            for key, value in updates.items():
                setattr(alert_model, key, value)
            alert_model.updated_at = dt.utcnow()
            db.commit()
            return
        except OperationalError as e:
            error_msg = str(e).lower()
            is_lock_error = (
                "database is locked" in error_msg or
                "database locked" in error_msg or
                "timeout" in error_msg or
                "locked" in error_msg
            )
            
            if not is_lock_error:
                # Not a lock error, re-raise immediately
                db.rollback()
                raise
            
            if attempt < max_retries:
                db.rollback()
                # Refresh model state
                db.refresh(alert_model)
                # Add jitter to prevent thundering herd
                jitter = random.uniform(0, delay * 0.1)
                time.sleep(delay + jitter)
                delay = min(delay * 2.0, max_delay)
                logger.warning(
                    "sentinel_dispatch.api.routers.websocket._update_alert_with_retry.retry",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(e),
                )
            else:
                # Max retries exceeded
                db.rollback()
                logger.error(
                    "sentinel_dispatch.api.routers.websocket._update_alert_with_retry.max_retries_exceeded",
                    error=str(e),
                    exc_info=True,
                )
                raise

router = APIRouter(tags=["websocket"])

connection_manager = get_connection_manager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.
    
    Message types sent to client:
    - risk_score_update: Risk score updates
    - new_call: New emergency calls
    - alert: Alert notifications
    - fire_update: Fire data updates
    - weather_update: Weather data updates
    
    Message types accepted from client:
    - acknowledge_alert: Acknowledge an alert
    - ping: Connection health check (responds with pong)
    """
    await connection_manager.connect(websocket)
    logger.info(
        "sentinel_dispatch.api.routers.websocket.websocket_endpoint.connected",
        total_connections=connection_manager.get_connection_count(),
    )
    
    try:
        while True:
            # Receive message from client
            try:
                data = await websocket.receive_text()
                message = json.loads(data)
                message_type = message.get("type")
                
                if message_type == "ping":
                    # Respond to ping with pong
                    await websocket.send_json({
                        "type": WebSocketMessageType.PONG,
                        "timestamp": message.get("timestamp"),
                    })
                
                elif message_type == "acknowledge_alert":
                    # Handle alert acknowledgment
                    from sentinel_dispatch.api.database import get_db_sync, AlertModel, alert_model_to_schema
                    
                    alert_id = message.get("alert_id")
                    if alert_id:
                        # Update alert in database
                        db = get_db_sync()
                        try:
                            alert_model = db.query(AlertModel).filter(AlertModel.alert_id == alert_id).first()
                            if alert_model:
                                # Update with retry for lock errors
                                _update_alert_with_retry(db, alert_model, {"acknowledged": True})
                                
                                updated_alert = alert_model_to_schema(alert_model)
                                
                                logger.info(
                                    "sentinel_dispatch.api.routers.websocket.websocket_endpoint.alert_acknowledged",
                                    alert_id=alert_id,
                                )
                                
                                # Broadcast updated alert
                                await connection_manager.broadcast_alert(updated_alert.model_dump())
                            else:
                                logger.warning(
                                    "sentinel_dispatch.api.routers.websocket.websocket_endpoint.alert_not_found",
                                    alert_id=alert_id,
                                )
                        except Exception as e:
                            db.rollback()
                            logger.error(
                                "sentinel_dispatch.api.routers.websocket.websocket_endpoint.acknowledge_error",
                                alert_id=alert_id,
                                error=str(e),
                                exc_info=True,
                            )
                        finally:
                            db.close()
                
                else:
                    logger.warning(
                        "sentinel_dispatch.api.routers.websocket.websocket_endpoint.unknown_message_type",
                        message_type=message_type,
                    )
            
            except WebSocketDisconnect:
                # Re-raise WebSocketDisconnect to exit the loop
                raise
            
            except json.JSONDecodeError:
                logger.warning(
                    "sentinel_dispatch.api.routers.websocket.websocket_endpoint.invalid_json",
                )
                continue
            
            except Exception as e:
                logger.error(
                    "sentinel_dispatch.api.routers.websocket.websocket_endpoint.message_error",
                    error=str(e),
                    exc_info=True,
                )
                continue
    
    except WebSocketDisconnect:
        logger.info(
            "sentinel_dispatch.api.routers.websocket.websocket_endpoint.disconnected",
        )
    except Exception as e:
        logger.error(
            "sentinel_dispatch.api.routers.websocket.websocket_endpoint.error",
            error=str(e),
            exc_info=True,
        )
    finally:
        await connection_manager.disconnect(websocket)


@router.get("/ws/status")
async def websocket_status():
    """Get WebSocket connection status."""
    return {
        "active_connections": connection_manager.get_connection_count(),
        "status": "active",
    }

