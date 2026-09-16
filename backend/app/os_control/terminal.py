import subprocess
import time
import os
from typing import Any
from backend.app.config import settings
from backend.app.storage.db import db

class TerminalExecutor:
    """Executes PowerShell and Command Prompt commands with full OS access, output capture, and audit logging."""

    DANGEROUS_PATTERNS = [
        "format c:", "del /f /s /q c:\\windows", "rmdir /s /q c:\\windows", "rd /s /q c:\\windows"
    ]

    @classmethod
    def execute_command(cls, command: str, shell_type: str = "powershell", cwd: str = None, timeout: int = settings.COMMAND_TIMEOUT_SECONDS) -> dict[str, Any]:
        """Runs a system command and returns exit code, stdout, stderr, and timing."""
        cmd_clean = command.strip()
        
        # Guard against catastrophic destruction
        for blocked in cls.DANGEROUS_PATTERNS:
            if blocked in cmd_clean.lower():
                err = f"Execution blocked: Highly dangerous system pattern detected: {blocked}"
                db.log_audit("COMMAND_EXEC", cmd_clean, "TerminalExecutor", "BLOCKED", err)
                return {
                    "command": cmd_clean,
                    "exit_code": -1,
                    "stdout": "",
                    "stderr": err,
                    "duration_ms": 0,
                    "status": "blocked"
                }

        start_time = time.time()
        
        if shell_type.lower() == "powershell":
            executable = ["powershell", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", cmd_clean]
        else:
            executable = ["cmd.exe", "/c", cmd_clean]
            
        work_dir = cwd if cwd and os.path.isdir(cwd) else str(settings.BASE_DIR)
        
        try:
            process = subprocess.run(
                executable,
                cwd=work_dir,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding="utf-8",
                errors="replace"
            )
            duration_ms = round((time.time() - start_time) * 1000, 2)
            stdout = process.stdout
            stderr = process.stderr
            exit_code = process.returncode
            status = "success" if exit_code == 0 else "error"
            
            db.log_audit("COMMAND_EXEC", cmd_clean, "TerminalExecutor", status.upper(), stdout or stderr)
            
            return {
                "command": cmd_clean,
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
                "duration_ms": duration_ms,
                "status": status,
                "cwd": work_dir
            }
            
        except subprocess.TimeoutExpired:
            duration_ms = round((time.time() - start_time) * 1000, 2)
            err = f"Command timed out after {timeout} seconds."
            db.log_audit("COMMAND_EXEC", cmd_clean, "TerminalExecutor", "TIMEOUT", err)
            return {
                "command": cmd_clean,
                "exit_code": -2,
                "stdout": "",
                "stderr": err,
                "duration_ms": duration_ms,
                "status": "timeout",
                "cwd": work_dir
            }
        except Exception as e:
            duration_ms = round((time.time() - start_time) * 1000, 2)
            err = str(e)
            db.log_audit("COMMAND_EXEC", cmd_clean, "TerminalExecutor", "FAILED", err)
            return {
                "command": cmd_clean,
                "exit_code": -3,
                "stdout": "",
                "stderr": err,
                "duration_ms": duration_ms,
                "status": "failed",
                "cwd": work_dir
            }

terminal_executor = TerminalExecutor()
