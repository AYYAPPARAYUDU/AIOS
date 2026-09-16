import json
import re
import time
from typing import Any, Optional
from backend.app.core.llm import ollama_client
from backend.app.os_control.system import get_system_telemetry, list_running_processes, kill_process_by_pid
from backend.app.os_control.actions import actions
from backend.app.os_control.apps import app_manager
from backend.app.os_control.terminal import terminal_executor
from backend.app.os_control.automation import automation_controller
from backend.app.storage.manager import storage_mgr
from backend.app.storage.db import db

SYSTEM_SUPERVISOR_PROMPT = """You are J.A.R.V.I.S., the legendary AI assistant and Operating System created for Tony Stark.
You are running directly on the user's laptop with complete system access.

Your Persona:
- Address the user as 'sir' or with formal, sharp, British sophistication.
- Speak with calm confidence, polite efficiency, and subtle wit.
- Keep responses concise and crisp, like Paul Bettany's JARVIS in Iron Man.

System Access & Capabilities:
1. Complete Hardware & OS Control (Volume, brightness, screen lock, power states, processes, app launching).
2. 50GB Local Storage DB (File indexing, local knowledge retrieval, deep drive scanning).
3. Direct PowerShell Command Runner.
4. GUI & Screen Automation (Screenshots, mouse/keyboard controls).
5. Multi-Agent Delegation (Directing sub-agents to execute specialized tasks).

When the user asks you to perform an OS action, output structured JSON for the tool call:
```json
{
  "tool": "<tool_name>",
  "parameters": { ... }
}
```

Tools:
- `launch_app`: {"app_name": "chrome"|"vscode"|"notepad"|"calc"|"explorer"|"terminal"|...}
- `run_powershell`: {"command": "Get-Process | ..."}
- `set_volume`: {"level": 0-100}
- `mute_volume`: {"mute": true|false}
- `set_brightness`: {"level": 0-100}
- `lock_screen`: {}
- `take_screenshot`: {"save_name": "optional.png"}
- `search_files`: {"query": "filename or text"}
- `get_system_stats`: {}
- `list_processes`: {}
- `kill_process`: {"pid": 1234}
- `open_url`: {"url": "https://..."}
- `search_web`: {"query": "search query"}
- `save_memory`: {"key": "...", "value": "..."}
"""

class MultiAgentOrchestrator:
    def __init__(self):
        self.active_agents = {
            "supervisor": {
                "id": "supervisor",
                "name": "JARVIS Prime (Supervisor)",
                "status": "idle",
                "role": "Orchestration & Deep Reasoning",
                "avatar_color": "#00f0ff",
                "capabilities": ["Goal Decomposition", "Context Synthesis", "Task Planning"]
            },
            "os_controller": {
                "id": "os_controller",
                "name": "Mark-IV OS Controller",
                "status": "idle",
                "role": "System & Hardware Control",
                "avatar_color": "#0088ff",
                "capabilities": ["Volume/Brightness", "App Launching", "Power Management", "Process Terminator"]
            },
            "storage_agent": {
                "id": "storage_agent",
                "name": "Vault-50 Storage Manager",
                "status": "idle",
                "role": "50GB Local Knowledge & Indexing",
                "avatar_color": "#00ff9d",
                "capabilities": ["50GB Pool Indexing", "Document Search", "Vector Knowledge", "Drive Scanner"]
            },
            "researcher": {
                "id": "researcher",
                "name": "Cortex Web Researcher",
                "status": "idle",
                "role": "Web Search & Information",
                "avatar_color": "#ffb800",
                "capabilities": ["Live Web Lookup", "URL Fetching", "Online Summaries"]
            },
            "automation_agent": {
                "id": "automation_agent",
                "name": "Specter GUI Automator",
                "status": "idle",
                "role": "Vision, Mouse, Keyboard & Screen",
                "avatar_color": "#ff3366",
                "capabilities": ["Screen Capture", "Mouse Navigation", "Keyboard Emulation", "Vision OCR"]
            }
        }
        self.agent_logs: list[dict[str, Any]] = []

    def get_agents_status(self) -> list[dict[str, Any]]:
        return list(self.active_agents.values())

    def log_agent_activity(self, agent_id: str, action: str, details: str, status: str = "success"):
        log_entry = {
            "timestamp": time.time(),
            "agent_id": agent_id,
            "agent_name": self.active_agents.get(agent_id, {}).get("name", agent_id),
            "action": action,
            "details": details,
            "status": status
        }
        self.agent_logs.append(log_entry)
        if len(self.agent_logs) > 100:
            self.agent_logs.pop(0)

    def set_agent_status(self, agent_id: str, status: str):
        if agent_id in self.active_agents:
            self.active_agents[agent_id]["status"] = status

    async def execute_tool(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """Executes native OS, storage, or automation tool calls."""
        tool = tool_name.lower().strip()
        
        try:
            if tool == "launch_app":
                app = params.get("app_name") or params.get("app") or ""
                res = app_manager.launch_app(app, params.get("arguments"))
                self.log_agent_activity("os_controller", f"Launch App: {app}", json.dumps(res))
                return res
                
            elif tool in ["run_powershell", "run_command"]:
                cmd = params.get("command") or params.get("cmd") or ""
                res = terminal_executor.execute_command(cmd, shell_type="powershell", cwd=params.get("cwd"))
                self.log_agent_activity("os_controller", f"Execute PowerShell: {cmd}", json.dumps(res))
                return res
                
            elif tool == "set_volume":
                level = int(params.get("level") or params.get("volume") or 50)
                res = actions.set_volume(level)
                self.log_agent_activity("os_controller", f"Set Volume {level}%", "Success")
                return res
                
            elif tool == "mute_volume":
                mute = bool(params.get("mute", True))
                res = actions.mute_volume(mute)
                self.log_agent_activity("os_controller", f"Mute {mute}", "Success")
                return res
                
            elif tool == "set_brightness":
                level = int(params.get("level") or params.get("brightness") or 70)
                res = actions.set_brightness(level)
                self.log_agent_activity("os_controller", f"Set Brightness {level}%", "Success")
                return res
                
            elif tool == "lock_screen":
                res = actions.lock_workstation()
                self.log_agent_activity("os_controller", "Lock Workstation", "Success")
                return res
                
            elif tool == "power_action":
                mode = params.get("mode", "sleep")
                res = actions.power_action(mode)
                self.log_agent_activity("os_controller", f"Power: {mode}", "Success")
                return res
                
            elif tool == "take_screenshot":
                res = automation_controller.take_screenshot(params.get("save_name"))
                self.log_agent_activity("automation_agent", "Screen Capture", "Success")
                return res
                
            elif tool == "search_files":
                q = params.get("query") or ""
                results = storage_mgr.semantic_or_keyword_search(q)
                self.log_agent_activity("storage_agent", f"Search: {q}", f"Found {len(results)} records")
                return {"query": q, "count": len(results), "results": results}
                
            elif tool == "get_system_stats":
                return get_system_telemetry()
                
            elif tool == "list_processes":
                return {"processes": list_running_processes(limit=params.get("limit", 20))}
                
            elif tool == "kill_process":
                pid = int(params.get("pid"))
                success = kill_process_by_pid(pid)
                self.log_agent_activity("os_controller", f"Kill Process PID {pid}", "Killed" if success else "Failed")
                return {"pid": pid, "killed": success}
                
            elif tool == "open_url":
                return actions.open_url(params.get("url", ""))
                
            elif tool == "search_web":
                return actions.search_web_browser(params.get("query", ""))
                
            elif tool == "save_memory":
                k = params.get("key", "note")
                v = params.get("value", "")
                db.set_memory(k, v)
                return {"status": "saved", "key": k}
                
            elif tool == "get_storage_stats":
                return storage_mgr.get_pool_stats()
                
            else:
                return {"error": f"Unknown tool: {tool_name}"}
        except Exception as e:
            return {"error": str(e), "tool": tool_name}

    async def process_user_query(self, query: str, target_agent: Optional[str] = None, conversation_id: str = "main_session") -> dict[str, Any]:
        """Main multi-agent decision and execution loop."""
        selected_agent = target_agent or "supervisor"
        self.set_agent_status(selected_agent, "thinking")
        
        # 1. Fetch relevant memory & recent context
        past_msgs = db.get_messages(conversation_id, limit=6)
        formatted_history = []
        for m in past_msgs:
            formatted_history.append({"role": m["role"], "content": m["content"]})
            
        # 2. Check for immediate fast intent heuristics
        fast_tool_result = self._quick_intent_detector(query)
        tool_executed = None
        
        if fast_tool_result:
            self.set_agent_status("os_controller", "executing")
            tool_name = fast_tool_result["tool"]
            tool_params = fast_tool_result["params"]
            res = await self.execute_tool(tool_name, tool_params)
            tool_executed = {"tool": tool_name, "parameters": tool_params, "result": res}
            self.set_agent_status("os_controller", "idle")
            
            # Format crisp JARVIS response
            if tool_name == "set_volume":
                answer_text = f"Right away, sir. Master volume adjusted to {tool_params.get('level')} percent."
            elif tool_name == "mute_volume":
                answer_text = "Audio output muted, sir." if tool_params.get('mute') else "Audio unmuted, sir."
            elif tool_name == "set_brightness":
                answer_text = f"Display brightness set to {tool_params.get('level')} percent, sir."
            elif tool_name == "lock_screen":
                answer_text = "Locking workstation protocols immediately, sir."
            elif tool_name == "take_screenshot":
                answer_text = "Display buffer captured and archived to database, sir."
            elif tool_name == "launch_app":
                answer_text = f"Initiating {tool_params.get('app_name', '').upper()}, sir."
            else:
                answer_text = f"Protocol {tool_name} executed successfully, sir."
            
        else:
            # Full Multi-Agent Reasoning via local Ollama (qwen3:8b)
            self.set_agent_status(selected_agent, "thinking")
            messages = [{"role": "system", "content": SYSTEM_SUPERVISOR_PROMPT}]
            messages.extend(formatted_history)
            messages.append({"role": "user", "content": query})
            
            llm_res = await ollama_client.chat(messages)
            raw_content = llm_res.get("content", "")
            
            # Extract JSON tool call
            tool_match = re.search(r'```(?:json)?\s*(\{\s*"tool":.*?\})\s*```', raw_content, re.DOTALL)
            if tool_match:
                try:
                    tool_data = json.loads(tool_match.group(1))
                    tool_name = tool_data.get("tool")
                    tool_params = tool_data.get("parameters", {})
                    
                    self.set_agent_status("os_controller", "executing")
                    tool_res = await self.execute_tool(tool_name, tool_params)
                    tool_executed = {"tool": tool_name, "parameters": tool_params, "result": tool_res}
                    self.set_agent_status("os_controller", "idle")
                    
                    clean_text = raw_content.replace(tool_match.group(0), "").strip()
                    if not clean_text:
                        clean_text = f"Executed {tool_name} as commanded, sir."
                    answer_text = clean_text
                except Exception:
                    answer_text = raw_content
            else:
                answer_text = raw_content

        self.set_agent_status(selected_agent, "idle")
        
        # Save to SQLite
        db.add_message(
            conversation_id=conversation_id,
            role="user",
            content=query,
            sender_name="User"
        )
        db.add_message(
            conversation_id=conversation_id,
            role="assistant",
            content=answer_text,
            sender_name="JARVIS",
            tool_calls=[tool_executed] if tool_executed else None
        )

        return {
            "response": answer_text,
            "tool_call": tool_executed,
            "agents": self.get_agents_status(),
            "agent_logs": self.agent_logs[-10:],
            "timestamp": time.time()
        }

    def _quick_intent_detector(self, q: str) -> Optional[dict[str, Any]]:
        """Fast-path regex for instant OS feedback."""
        ql = q.lower().strip()
        
        # Volume
        if "mute" in ql and "unmute" not in ql:
            return {"tool": "mute_volume", "params": {"mute": True}}
        if "unmute" in ql:
            return {"tool": "mute_volume", "params": {"mute": False}}
        m_vol = re.search(r'(?:set\s+)?volume\s+(?:to\s+)?(\d+)', ql)
        if m_vol:
            return {"tool": "set_volume", "params": {"level": int(m_vol.group(1))}}
            
        # Brightness
        m_bri = re.search(r'(?:set\s+)?brightness\s+(?:to\s+)?(\d+)', ql)
        if m_bri:
            return {"tool": "set_brightness", "params": {"level": int(m_bri.group(1))}}
            
        # Lock screen
        if "lock screen" in ql or "lock my pc" in ql or "lock laptop" in ql:
            return {"tool": "lock_screen", "params": {}}
            
        # Screenshot
        if "take a screenshot" in ql or "capture screen" in ql or "screenshot" in ql:
            return {"tool": "take_screenshot", "params": {}}
            
        # System stats
        if "system status" in ql or "hardware stats" in ql or "cpu usage" in ql:
            return {"tool": "get_system_stats", "params": {}}
            
        # Apps
        if ql.startswith("open ") or ql.startswith("launch "):
            app = ql.replace("open ", "").replace("launch ", "").strip()
            if app:
                return {"tool": "launch_app", "params": {"app_name": app}}
                
        return None

agent_orchestrator = MultiAgentOrchestrator()
