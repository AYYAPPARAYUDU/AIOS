"""
Dynamic Storage Vault Sentinel & Capacity Monitor
Monitors storage consumption across database directories, calculates available quota (200GB dynamic pool),
and triggers capacity alerts when approaching limits to request user storage expansion.
"""

import os
import shutil
from typing import Dict, Any, List

DATABASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../database"))

class CapacityMonitor:
    def __init__(self, allocated_gb: float = 200.0):
        self.allocated_gb = allocated_gb
        self.alert_threshold_pct = 85.0

    def get_dir_size(self, path: str) -> int:
        total = 0
        if not os.path.exists(path):
            return 0
        try:
            for entry in os.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat().st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += self.get_dir_size(entry.path)
        except Exception:
            pass
        return total

    def check_capacity(self) -> Dict[str, Any]:
        """Calculates precise breakdown of the storage vault."""
        subdirs = [
            {"name": "Clone Neural Memory", "path": os.path.join(DATABASE_DIR, "clone_memory"), "icon": "🧠"},
            {"name": "Evolution Metrics", "path": os.path.join(DATABASE_DIR, "evolution_metrics"), "icon": "📈"},
            {"name": "Knowledge & Doc Vault", "path": os.path.join(DATABASE_DIR, "knowledge_vault"), "icon": "📚"},
            {"name": "Generated 4K Media", "path": os.path.join(DATABASE_DIR, "generated_media"), "icon": "🎨"},
            {"name": "Vision & Audio Logs", "path": os.path.join(DATABASE_DIR, "vision_logs"), "icon": "👁️"},
            {"name": "Screenshots & Audit", "path": os.path.join(DATABASE_DIR, "screenshots"), "icon": "📸"}
        ]

        total_bytes = 0
        breakdown = []
        for s in subdirs:
            bytes_size = self.get_dir_size(s["path"])
            total_bytes += bytes_size
            breakdown.append({
                "name": s["name"],
                "icon": s["icon"],
                "bytes": bytes_size,
                "mb": round(bytes_size / (1024 * 1024), 2),
                "formatted": f"{round(bytes_size / (1024 * 1024), 2)} MB" if bytes_size > 1024*1024 else f"{round(bytes_size / 1024, 2)} KB"
            })

        # Calculate quota
        allocated_bytes = self.allocated_gb * 1024 * 1024 * 1024
        used_gb = round(total_bytes / (1024 * 1024 * 1024), 4)
        available_gb = round(self.allocated_gb - used_gb, 2)
        usage_pct = round((total_bytes / allocated_bytes) * 100, 2)

        # Alert trigger
        is_alert = usage_pct >= self.alert_threshold_pct
        alert_message = None
        if is_alert:
            alert_message = f"CRITICAL: Storage vault is at {usage_pct}% capacity. Please expand storage allocation (+100GB) or execute neural vector pruning."

        return {
            "status": "healthy" if not is_alert else "warning",
            "allocated_gb": self.allocated_gb,
            "used_gb": used_gb,
            "used_mb": round(total_bytes / (1024 * 1024), 2),
            "available_gb": available_gb,
            "usage_percentage": usage_pct,
            "requires_user_expansion": is_alert,
            "alert_message": alert_message,
            "breakdown": breakdown,
            "vault_root": DATABASE_DIR
        }

    def expand_storage(self, additional_gb: float = 100.0) -> Dict[str, Any]:
        """Expands storage allocation quota on user confirmation."""
        self.allocated_gb += additional_gb
        return {
            "status": "success",
            "new_allocation_gb": self.allocated_gb,
            "message": f"Storage vault successfully expanded to {self.allocated_gb} GB."
        }

capacity_monitor = CapacityMonitor()
