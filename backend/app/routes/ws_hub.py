import asyncio
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from backend.app.os_control.system import get_system_telemetry
from backend.app.core.agents import agent_orchestrator
from backend.app.core.emotion import emotion_engine

logger = logging.getLogger(__name__)
router = APIRouter(tags=["WebSocket Hub"])

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

ws_manager = ConnectionManager()

@router.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            telemetry = get_system_telemetry()
            telemetry["emotion"] = emotion_engine.get_current_emotion()
            agents = agent_orchestrator.get_agents_status()
            
            payload = {
                "type": "TELEMETRY_UPDATE",
                "telemetry": telemetry,
                "agents": agents
            }
            await websocket.send_json(payload)
            await asyncio.sleep(1.5)
            
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        ws_manager.disconnect(websocket)
