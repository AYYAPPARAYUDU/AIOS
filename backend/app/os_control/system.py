import os
import sys
import time
import socket
import subprocess
import psutil
from typing import Any
from backend.app.storage.manager import storage_mgr

_last_net_io = None
_last_net_time = None

def get_system_telemetry() -> dict[str, Any]:
    """Collects comprehensive hardware and OS metrics."""
    global _last_net_io, _last_net_time
    
    # CPU
    cpu_percent = psutil.cpu_percent(interval=None)
    cpu_count_logical = psutil.cpu_count(logical=True)
    cpu_count_physical = psutil.cpu_count(logical=False)
    cpu_freq = psutil.cpu_freq()
    
    # Memory
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    
    # Battery
    battery_info = None
    try:
        bat = psutil.sensors_battery()
        if bat:
            battery_info = {
                "percent": bat.percent,
                "power_plugged": bat.power_plugged,
                "secsleft": bat.secsleft if bat.secsleft != psutil.POWER_TIME_UNLIMITED else -1
            }
    except Exception:
        pass

    # Disks
    disks = []
    for part in psutil.disk_partitions(all=False):
        if os.name == 'nt' and ('cdrom' in part.opts or part.fstype == ''):
            continue
        try:
            usage = psutil.disk_usage(part.mountpoint)
            disks.append({
                "device": part.device,
                "mountpoint": part.mountpoint,
                "fstype": part.fstype,
                "total_gb": round(usage.total / (1024**3), 2),
                "used_gb": round(usage.used / (1024**3), 2),
                "free_gb": round(usage.free / (1024**3), 2),
                "percent": usage.percent
            })
        except Exception:
            pass

    # Network Speed Calculation
    net_io = psutil.net_io_counters()
    now = time.time()
    download_speed_kbps = 0.0
    upload_speed_kbps = 0.0
    
    if _last_net_io and _last_net_time:
        dt = now - _last_net_time
        if dt > 0:
            download_speed_kbps = round(((net_io.bytes_recv - _last_net_io.bytes_recv) / 1024) / dt, 2)
            upload_speed_kbps = round(((net_io.bytes_sent - _last_net_io.bytes_sent) / 1024) / dt, 2)
            
    _last_net_io = net_io
    _last_net_time = now

    # Host IP & Name
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        hostname = "JARVIS-HOST"
        local_ip = "127.0.0.1"

    # 50GB Local Storage DB Stats
    storage_pool = storage_mgr.get_pool_stats()

    # GPU info (quick query if on windows)
    gpu_info = get_gpu_info()

    return {
        "timestamp": now,
        "os": {
            "system": sys.platform,
            "hostname": hostname,
            "local_ip": local_ip,
            "boot_time": psutil.boot_time(),
            "uptime_seconds": round(now - psutil.boot_time(), 0)
        },
        "cpu": {
            "percent": cpu_percent,
            "cores_logical": cpu_count_logical,
            "cores_physical": cpu_count_physical,
            "current_freq_mhz": round(cpu_freq.current, 1) if cpu_freq else 0
        },
        "memory": {
            "total_gb": round(mem.total / (1024**3), 2),
            "used_gb": round(mem.used / (1024**3), 2),
            "free_gb": round(mem.available / (1024**3), 2),
            "percent": mem.percent,
            "swap_percent": swap.percent
        },
        "battery": battery_info,
        "disks": disks,
        "storage_pool_50gb": storage_pool,
        "network": {
            "bytes_sent": net_io.bytes_sent,
            "bytes_recv": net_io.bytes_recv,
            "upload_speed_kbps": upload_speed_kbps,
            "download_speed_kbps": download_speed_kbps
        },
        "gpu": gpu_info
    }

def get_gpu_info() -> list[dict[str, Any]]:
    """Tries to query GPU metrics on Windows or via nvidia-smi."""
    gpus = []
    # Try nvidia-smi
    try:
        res = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total,memory.used,utilization.gpu,temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=2
        )
        if res.returncode == 0 and res.stdout.strip():
            for line in res.stdout.strip().split("\n"):
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 5:
                    gpus.append({
                        "name": parts[0],
                        "total_mem_mb": float(parts[1]),
                        "used_mem_mb": float(parts[2]),
                        "utilization_percent": float(parts[3]),
                        "temp_c": float(parts[4])
                    })
            return gpus
    except Exception:
        pass

    # Windows WMIC fallback for GPU Name
    if os.name == "nt":
        try:
            res = subprocess.run(
                ["powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_VideoController | Select-Object -Property Name, AdapterRAM | ConvertTo-Json"],
                capture_output=True, text=True, timeout=3
            )
            if res.returncode == 0 and res.stdout.strip():
                import json
                data = json.loads(res.stdout)
                if isinstance(data, dict):
                    data = [data]
                for item in data:
                    name = item.get("Name", "Generic GPU")
                    ram_bytes = item.get("AdapterRAM") or 0
                    gpus.append({
                        "name": name,
                        "total_mem_mb": round(ram_bytes / (1024**2), 1) if ram_bytes else 0,
                        "used_mem_mb": 0,
                        "utilization_percent": 0,
                        "temp_c": 0
                    })
        except Exception:
            pass

    return gpus

def list_running_processes(limit: int = 30) -> list[dict[str, Any]]:
    """Returns top active processes sorted by CPU and memory usage."""
    procs = []
    for p in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status', 'create_time']):
        try:
            info = p.info
            procs.append({
                "pid": info['pid'],
                "name": info['name'] or "unknown",
                "cpu_percent": round(info['cpu_percent'] or 0, 1),
                "memory_percent": round(info['memory_percent'] or 0, 1),
                "status": info['status'],
                "uptime": round(time.time() - info['create_time'], 0) if info.get('create_time') else 0
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    # Sort primarily by cpu_percent then memory_percent
    procs.sort(key=lambda x: (x['cpu_percent'], x['memory_percent']), reverse=True)
    return procs[:limit]

def kill_process_by_pid(pid: int) -> bool:
    """Terminates a process by PID."""
    try:
        proc = psutil.Process(pid)
        proc.terminate()
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        # Try force kill via taskkill if on Windows
        if os.name == 'nt':
            try:
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, timeout=3)
                return True
            except Exception:
                pass
        raise e
