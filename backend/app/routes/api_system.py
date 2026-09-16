from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Any
from backend.app.os_control.system import get_system_telemetry, list_running_processes, kill_process_by_pid
from backend.app.os_control.actions import actions
from backend.app.os_control.apps import app_manager
from backend.app.os_control.terminal import terminal_executor
from backend.app.os_control.automation import automation_controller
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

class FocusWindowRequest(BaseModel):
    title_keyword: str

class TerminalExecuteRequest(BaseModel):
    command: str
    shell_type: str = "powershell"
    cwd: Optional[str] = None
    timeout: Optional[int] = 45

class ScreenshotRequest(BaseModel):
    save_name: Optional[str] = None

class ClickRequest(BaseModel):
    x: Optional[int] = None
    y: Optional[int] = None
    clicks: int = 1
    button: str = "left"

class TypeTextRequest(BaseModel):
    text: str
    interval: float = 0.02

class HotkeyRequest(BaseModel):
    keys: list[str]

class ProcessKillRequest(BaseModel):
    pid: int

# Endpoints
@router.get("/telemetry")
async def get_telemetry():
    return get_system_telemetry()

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

@router.post("/actions/media")
async def handle_media(action: str = Query(...)):
    return actions.media_control(action)

@router.post("/actions/app")
async def launch_application(req: AppLaunchRequest):
    return app_manager.launch_app(req.app_name, req.arguments)

@router.get("/windows")
async def get_windows():
    return {"windows": app_manager.get_open_windows()}

@router.post("/windows/focus")
async def focus_window(req: FocusWindowRequest):
    return app_manager.focus_window(req.title_keyword)

@router.post("/terminal/execute")
async def execute_command(req: TerminalExecuteRequest):
    return terminal_executor.execute_command(
        command=req.command,
        shell_type=req.shell_type,
        cwd=req.cwd,
        timeout=req.timeout or 45
    )

@router.post("/automation/screenshot")
async def take_screenshot(req: ScreenshotRequest):
    return automation_controller.take_screenshot(req.save_name)

@router.get("/automation/screen-info")
async def get_screen_info():
    return automation_controller.get_screen_info()

@router.post("/automation/click")
async def gui_click(req: ClickRequest):
    return automation_controller.click(req.x, req.y, req.clicks, req.button)

@router.post("/automation/type")
async def gui_type(req: TypeTextRequest):
    return automation_controller.type_text(req.text, req.interval)

@router.post("/automation/hotkey")
async def gui_hotkey(req: HotkeyRequest):
    return automation_controller.press_hotkey(req.keys)

@router.get("/audit")
async def get_audit_logs(limit: int = 50):
    return {"logs": db.get_audit_logs(limit=limit)}
