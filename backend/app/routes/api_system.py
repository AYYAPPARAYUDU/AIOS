from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Any
from backend.app.os_control.system import get_system_telemetry, list_running_processes, kill_process_by_pid
from backend.app.os_control.actions import actions
from backend.app.os_control.apps import app_manager
from backend.app.os_control.terminal import terminal_executor
from backend.app.os_control.automation import automation_controller
from backend.app.os_control.optimizer import system_optimizer
from backend.app.core.emotion import emotion_engine
from backend.app.storage.db import db

router = APIRouter(prefix="/api/system", tags=["System & OS Controls"])

# Request models
class VolumeRequest(BaseModel):
    level: Optional[int] = None
    mute: Optional[bool] = None

class BrightnessRequest(BaseModel):
    level: int

class PowerRequest(BaseModel):
    mode: str  # sleep, restart, shutdown, lock, cancel_shutdown

class AppLaunchRequest(BaseModel):
    app_name: str
    arguments: Optional[str] = None

class MacroRequest(BaseModel):
    macro_name: str

class TerminalExecuteRequest(BaseModel):
    command: str
    shell_type: str = "powershell"
    cwd: Optional[str] = None
    timeout: Optional[int] = 45

class ProcessKillRequest(BaseModel):
    pid: int

# Endpoints
@router.get("/telemetry")
async def get_telemetry():
    tel = get_system_telemetry()
    tel["emotion"] = emotion_engine.get_current_emotion()
    return tel

@router.get("/emotion")
async def get_user_emotion():
    return emotion_engine.get_current_emotion()

@router.post("/macro")
async def run_system_macro(req: MacroRequest):
    return system_optimizer.execute_macro(req.macro_name)

@router.post("/purge-ram")
async def purge_system_ram():
    return system_optimizer.purge_ram()

@router.get("/processes")
async def get_processes(limit: int = Query(default=30, le=100)):
    return {"processes": list_running_processes(limit=limit)}

@router.post("/processes/kill")
async def kill_process(req: ProcessKillRequest):
    try:
        success = kill_process_by_pid(req.pid)
        return {"status": "success", "pid": req.pid, "killed": success}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/actions/volume")
async def set_system_volume(req: VolumeRequest):
    if req.mute is not None:
        return actions.mute_volume(req.mute)
    if req.level is not None:
        return actions.set_volume(req.level)
    return {"status": "no_change"}

@router.post("/actions/brightness")
async def set_system_brightness(req: BrightnessRequest):
    return actions.set_brightness(req.level)

@router.post("/actions/power")
async def handle_power(req: PowerRequest):
    if req.mode == "lock":
        return actions.lock_workstation()
    return actions.power_action(req.mode)

@router.post("/actions/app")
async def launch_application(req: AppLaunchRequest):
    return app_manager.launch_app(req.app_name, req.arguments)

@router.post("/terminal/execute")
async def execute_command(req: TerminalExecuteRequest):
    return terminal_executor.execute_command(
        command=req.command,
        shell_type=req.shell_type,
        cwd=req.cwd,
        timeout=req.timeout or 45
    )

@router.post("/automation/screenshot")
async def take_screenshot():
    return automation_controller.take_screenshot()

@router.get("/audit")
async def get_audit_logs(limit: int = 50):
    return {"logs": db.get_audit_logs(limit=limit)}
