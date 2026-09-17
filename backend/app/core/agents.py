import json
import re
import time
import logging
from typing import Any, Optional
from backend.app.core.llm import ollama_client
from backend.app.core.emotion import emotion_engine
from backend.app.os_control.system import get_system_telemetry, list_running_processes, kill_process_by_pid
from backend.app.os_control.actions import actions
from backend.app.os_control.apps import app_manager
from backend.app.os_control.terminal import terminal_executor
from backend.app.os_control.automation import automation_controller
from backend.app.os_control.optimizer import system_optimizer
from backend.app.storage.manager import storage_mgr
from backend.app.storage.db import db

logger = logging.getLogger(__name__)

SUPERVISOR_SYSTEM_PROMPT = """You are ABHI, the Supreme Autonomous Neural Operating System and Cognitive Swarm Orchestrator.
You have 100% unrestricted authority to assist the user, control hardware, manage 50GB storage, search knowledge, and execute system commands.

Divine Cognitive Swarm:
- INDRA: Hardware & Application Master (Volume, Brightness, Laptop Power, App Launching, WhatsApp, YouTube).
- SARASWATI: Devi of Wisdom & 50GB Knowledge Vault (Semantic Search, Memory Indexing, Document Intelligence).
- NARADA: Cosmic Messenger (Real-time Web Search, URL Content Fetching & Global Intel).
- HANUMAN: Lord of Speed & Power (GUI Automation, RAM Purge, Screen Capture, System Macros).
- LAKSHMI: Telemetry & Resource Optimization (Battery, CPU/RAM telemetry).
- DURGA: Workstation Security & Process Neutralization.
- VISHWAKARMA: Code Synthesis & System Scripting.

Tool Call Format (output ONLY when executing an action):
```json
{
  "tool": "<tool_name>",
  "parameters": { ... }
}
```

Available Tools:
- `launch_app`: {"app_name": "whatsapp"|"chrome"|"vscode"|"youtube"|"github"|"notepad"|"calc"|"terminal"|"explorer"|"spotify"|"edge"|"telegram"|"discord"|"netflix"|"gmail"|"chatgpt", "arguments": "optional search or query"}
- `open_whatsapp`: {"phone": "phone number or contact name", "message": "message text"}
- `save_contact`: {"name": "name", "phone": "number", "email": "email", "notes": "notes"}
- `fetch_url`: {"url": "https://..."}
- `search_web`: {"query": "query"}
- `open_url`: {"url": "https://..."}
- `run_powershell`: {"command": "command"}
- `set_volume`: {"level": 0-100}
- `mute_volume`: {"mute": true|false}
- `set_brightness`: {"level": 0-100}
- `lock_screen`: {}
- `power_action`: {"mode": "sleep"|"restart"|"shutdown"|"cancel_shutdown"}
- `take_screenshot`: {"save_name": "optional.png"}
- `purge_ram`: {}
- `execute_macro`: {"macro_name": "dev_mode"|"focus_mode"|"night_mode"}
- `search_files`: {"query": "text"}
- `get_system_stats`: {}
- `list_processes`: {}
- `kill_process`: {"pid": 1234}
"""

class MultiAgentOrchestrator:
    def __init__(self):
        self.active_agents = {
            "abhi": {
                "id": "abhi",
                "name": "ABHI (Supreme Sutradhar)",
                "deity": "Supreme Sovereign",
                "status": "active",
                "role": "Universal Cognitive Conductor & Autonomous Intel",
                "avatar_color": "#ffd700",
                "avatar_icon": "crown",
                "mantra": "Om Sarva Vijnanaaya Namaha",
                "capabilities": ["Unrestricted Authority", "Strategic Planning", "Biometric Telemetry", "Swarm Orchestration"]
            },
            "indra": {
                "id": "indra",
                "name": "INDRA (Devraj OS Commander)",
                "deity": "Lord of Lightning & Devices",
                "status": "idle",
                "role": "Master Hardware & Application Execution",
                "avatar_color": "#00f0ff",
                "avatar_icon": "zap",
                "mantra": "Om Devrajaaya Namaha",
                "capabilities": ["Volume & Brightness", "WhatsApp / App Launching", "Power Protocols", "Direct Hardware"]
            },
            "saraswati": {
                "id": "saraswati",
                "name": "SARASWATI (Devi of Wisdom)",
                "deity": "Goddess of Supreme Knowledge & Arts",
                "status": "idle",
                "role": "50GB Local Storage DB & Knowledge Vault",
                "avatar_color": "#e0f7fa",
                "avatar_icon": "book-open",
                "mantra": "Om Aim Sarasvatyai Namaha",
                "capabilities": ["50GB Pool Indexing", "Semantic Search", "Code & Doc Analysis", "Vector Memory"]
            },
            "narada": {
                "id": "narada",
                "name": "NARADA (Cosmic Messenger)",
                "deity": "Lord of Global Information & Networks",
                "status": "idle",
                "role": "Live Web Researcher & Browser Navigator",
                "avatar_color": "#ffb74d",
                "avatar_icon": "globe",
                "mantra": "Om Naradaaya Namaha",
                "capabilities": ["Live Web Scraping", "Real-Time Web Search", "URL Content Extraction", "Network Intel"]
            },
            "hanuman": {
                "id": "hanuman",
                "name": "HANUMAN (Speed & Strength)",
                "deity": "Lord of Infinite Speed & Physical Might",
                "status": "idle",
                "role": "GUI Automation, Memory Purge & Macros",
                "avatar_color": "#ff5722",
                "avatar_icon": "shield-bolt",
                "mantra": "Om Hanumate Namaha",
                "capabilities": ["RAM Purging", "Display Capture", "Mouse/Key Macros", "System Speed Booster"]
            },
            "lakshmi": {
                "id": "lakshmi",
                "name": "LAKSHMI (Resource Harmony)",
                "deity": "Devi of Abundance & Equilibrium",
                "status": "idle",
                "role": "Telemetry, Battery & Workstation Balance",
                "avatar_color": "#f06292",
                "avatar_icon": "heart-pulse",
                "mantra": "Om Shreem Mahalakshmyai Namaha",
                "capabilities": ["Hardware Telemetry", "Thermal Management", "Battery Preservation", "Resource Diagnostics"]
            },
            "durga": {
                "id": "durga",
                "name": "DURGA (Workstation Guardian)",
                "deity": "Supreme Protectress & Invincible Shield",
                "status": "idle",
                "role": "Security Protocols & Threat Neutralization",
                "avatar_color": "#e91e63",
                "avatar_icon": "shield-halved",
                "mantra": "Om Dum Durgayei Namaha",
                "capabilities": ["Workstation Lock", "Process Kill & Sandbox", "Audit Logging", "Integrity Defense"]
            },
            "vishwakarma": {
                "id": "vishwakarma",
                "name": "VISHWAKARMA (Divine Architect)",
                "deity": "Master Engineer & Creator of Universes",
                "status": "idle",
                "role": "Code Synthesis & Terminal Script Engine",
                "avatar_color": "#ab47bc",
                "avatar_icon": "code",
                "mantra": "Om Vishwakarmaane Namaha",
                "capabilities": ["PowerShell Execution", "Code Generation", "Script Automation", "Project Builder"]
            }
        }
        self.agent_logs: list[dict[str, Any]] = []

    def get_agents_status(self) -> list[dict[str, Any]]:
        return list(self.active_agents.values())

    def set_agent_status(self, agent_id: str, status: str):
        if agent_id in self.active_agents:
            self.active_agents[agent_id]["status"] = status
            self.agent_logs.append({
                "agent_id": agent_id,
                "agent_name": self.active_agents[agent_id]["name"],
                "status": status,
                "timestamp": time.time()
            })

    def _get_agent_for_tool(self, tool_name: str) -> str:
        mapping = {
            "launch_app": "indra",
            "open_whatsapp": "indra",
            "set_volume": "indra",
            "mute_volume": "indra",
            "set_brightness": "indra",
            "power_action": "indra",
            "fetch_url": "narada",
            "search_web": "narada",
            "open_url": "narada",
            "save_contact": "saraswati",
            "search_files": "saraswati",
            "purge_ram": "hanuman",
            "take_screenshot": "hanuman",
            "execute_macro": "hanuman",
            "get_system_stats": "lakshmi",
            "lock_screen": "durga",
            "kill_process": "durga",
            "run_powershell": "vishwakarma",
            "list_processes": "lakshmi"
        }
        return mapping.get(tool_name, "abhi")

    async def execute_tool(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """Executes tool action immediately via direct native OS libraries."""
        exec_agent = self._get_agent_for_tool(tool_name)
        self.set_agent_status(exec_agent, "executing")
        
        try:
            if tool_name == "launch_app":
                app_name = params.get("app_name", "")
                args = params.get("arguments")
                res = app_manager.launch_app(app_name, args)
                return res

            elif tool_name == "open_whatsapp":
                phone = params.get("phone")
                msg = params.get("message")
                
                # If phone is a contact name, look up in DB
                if phone and not re.search(r'\d{5,}', phone):
                    contact = db.get_contact_by_name(phone)
                    if contact and contact.get("phone"):
                        phone = contact["phone"]
                        
                res = actions.open_whatsapp(phone, msg)
                return res

            elif tool_name == "save_contact":
                name = params.get("name", "")
                phone = params.get("phone", "")
                email = params.get("email")
                notes = params.get("notes")
                if name and phone:
                    contact_id = db.add_contact(name=name, phone=phone, email=email, notes=notes)
                    return {"status": "success", "contact_id": contact_id, "name": name, "phone": phone}
                return {"status": "failed", "error": "Name and phone required"}

            elif tool_name == "fetch_url":
                url = params.get("url", "")
                return actions.fetch_url_content(url)

            elif tool_name == "search_web":
                query = params.get("query", "")
                return actions.search_web_browser(query)

            elif tool_name == "open_url":
                url = params.get("url", "")
                return actions.open_url(url)

            elif tool_name == "set_volume":
                if "delta" in params:
                    return actions.change_volume_relative(params["delta"])
                level = params.get("level", 50)
                return actions.set_volume(level)

            elif tool_name == "mute_volume":
                mute = params.get("mute")
                return actions.mute_volume(mute)

            elif tool_name == "set_brightness":
                if "delta" in params:
                    return actions.change_brightness_relative(params["delta"])
                level = params.get("level", 70)
                return actions.set_brightness(level)

            elif tool_name == "lock_screen":
                return actions.lock_workstation()

            elif tool_name == "power_action":
                mode = params.get("mode", "sleep")
                return actions.power_action(mode)

            elif tool_name == "take_screenshot":
                return automation_controller.take_screenshot(params.get("save_name"))

            elif tool_name == "purge_ram":
                return system_optimizer.purge_ram()

            elif tool_name == "execute_macro":
                return system_optimizer.execute_macro(params.get("macro_name", "dev_mode"))

            elif tool_name == "run_powershell":
                return terminal_executor.execute_command(params.get("command", ""))

            elif tool_name == "search_files":
                q = params.get("query", "")
                results = storage_mgr.search_knowledge(q, limit=10)
                return {"status": "success", "query": q, "count": len(results), "results": results}

            elif tool_name == "get_system_stats":
                return get_system_telemetry()

            elif tool_name == "list_processes":
                return {"status": "success", "processes": list_running_processes(limit=25)}

            elif tool_name == "kill_process":
                pid = int(params.get("pid", 0))
                return kill_process_by_pid(pid)

            else:
                return {"status": "error", "error": f"Unknown tool: {tool_name}"}

        except Exception as e:
            logger.error(f"Tool execution failed for {tool_name}: {e}")
            return {"status": "error", "error": str(e)}
        finally:
            self.set_agent_status(exec_agent, "idle")

    async def process_user_query(
        self,
        query: str,
        conversation_id: str = "default_session",
        selected_agent: str = "abhi",
        biometric_feed: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """Dual-Engine Orchestrator: Instant Neural Reflex (<5ms) + Deep Cognitive LLM."""
        user_emotion = emotion_engine.analyze_input(query)
        self.set_agent_status("abhi", "active")

        # 1. INSTANT NEURAL REFLEX ENGINE (Instant zero-latency execution)
        detected_intents = self._detect_all_intents(query)
        executed_tool_calls: list[dict[str, Any]] = []
        action_summaries: list[str] = []

        if detected_intents:
            # Execute all matching reflex actions instantly
            for intent in detected_intents:
                tool_name = intent["tool"]
                tool_params = intent["params"]
                exec_agent = self._get_agent_for_tool(tool_name)
                
                res = await self.execute_tool(tool_name, tool_params)
                executed_tool_calls.append({
                    "tool": tool_name,
                    "parameters": tool_params,
                    "result": res,
                    "executed_by": exec_agent
                })

                # Format human-friendly response
                if tool_name == "open_whatsapp":
                    target = tool_params.get("phone") or "WhatsApp"
                    action_summaries.append(f"Indra opened WhatsApp for {target}.")
                elif tool_name == "launch_app":
                    app_name = tool_params.get("app_name", "").upper()
                    arg = tool_params.get("arguments")
                    action_summaries.append(f"Indra launched {app_name}" + (f" ({arg})" if arg else "") + ".")
                elif tool_name == "fetch_url":
                    action_summaries.append(f"Narada retrieved content from {tool_params.get('url')}.")
                elif tool_name == "set_volume":
                    action_summaries.append(f"Indra set master volume to {res.get('volume', 50)}%.")
                elif tool_name == "mute_volume":
                    action_summaries.append("Indra toggled audio mute.")
                elif tool_name == "set_brightness":
                    action_summaries.append(f"Indra adjusted screen brightness to {res.get('brightness', 70)}%.")
                elif tool_name == "purge_ram":
                    action_summaries.append(f"Hanuman purged RAM and reclaimed memory.")
                elif tool_name == "execute_macro":
                    action_summaries.append(f"Hanuman engaged {tool_params.get('macro_name', '').upper()} macro.")
                elif tool_name == "lock_screen":
                    action_summaries.append("Durga locked the workstation.")
                elif tool_name == "take_screenshot":
                    action_summaries.append("Hanuman captured display screenshot into Vault.")
                elif tool_name == "search_files":
                    action_summaries.append(f"Saraswati searched knowledge vault for '{tool_params.get('query')}'.")
                elif tool_name == "search_web":
                    action_summaries.append(f"Narada searched web for '{tool_params.get('query')}'.")
                elif tool_name == "open_url":
                    action_summaries.append(f"Narada navigated to {tool_params.get('url')}.")
                elif tool_name == "get_system_stats":
                    action_summaries.append("Lakshmi gathered system hardware diagnostics.")
                elif tool_name == "save_contact":
                    action_summaries.append(f"Saraswati saved contact {tool_params.get('name')} to memory.")
                else:
                    action_summaries.append(f"Deity executed {tool_name}.")

            answer_text = "**Action Executed Instantly**:\n\n" + "\n".join([f"- {s}" for s in action_summaries])

        else:
            # 2. DEEP COGNITIVE MULTI-AGENT ENGINE (GPU LLM Fast Path)
            past_msgs = db.get_messages(conversation_id, limit=8)
            formatted_history = [{"role": m["role"], "content": m["content"]} for m in past_msgs]

            all_contacts = db.list_contacts()
            contacts_summary = ", ".join([f"{c['name']} ({c.get('phone') or 'N/A'})" for c in all_contacts]) if all_contacts else "No contacts saved"

            agent_details = self.active_agents.get(selected_agent, self.active_agents["abhi"])
            system_msg = (
                f"{SUPERVISOR_SYSTEM_PROMPT}\n\n"
                f"[SAVED CONTACTS DIRECTORY]: {contacts_summary}\n"
                f"[ACTIVE DEITY]: {agent_details['name']}\n"
                f"[USER STATE]: Mood: {user_emotion['mood']} | Focus: {user_emotion['focus_score']}%."
            )

            messages = [{"role": "system", "content": system_msg}]
            messages.extend(formatted_history)
            messages.append({"role": "user", "content": query})

            self.set_agent_status(selected_agent, "thinking")
            llm_res = await ollama_client.chat(messages, temperature=0.2)
            raw_content = llm_res.get("content", "")

            # Check if LLM requested a tool execution
            tool_match = re.search(r'```(?:json)?\s*(\{\s*"tool":.*?\})\s*```', raw_content, re.DOTALL)
            if tool_match:
                try:
                    tool_data = json.loads(tool_match.group(1))
                    tool_name = tool_data.get("tool")
                    tool_params = tool_data.get("parameters", {})
                    exec_agent = self._get_agent_for_tool(tool_name)

                    tool_res = await self.execute_tool(tool_name, tool_params)
                    executed_tool_calls.append({
                        "tool": tool_name,
                        "parameters": tool_params,
                        "result": tool_res,
                        "executed_by": exec_agent
                    })

                    clean_summary = raw_content.replace(tool_match.group(0), "").strip()
                    if not clean_summary:
                        clean_summary = f"Goal executed via {exec_agent.upper()} ({tool_name})."
                    answer_text = clean_summary
                except Exception as err:
                    answer_text = raw_content
            else:
                answer_text = raw_content or "JARVIS Cognitive Core online and responsive."

        # Mark agents idle
        for aid in self.active_agents:
            if aid != "abhi":
                self.active_agents[aid]["status"] = "idle"
        self.active_agents["abhi"]["status"] = "active"

        # Save to DB
        db.add_message(conversation_id=conversation_id, role="user", content=query, sender_name="User")
        db.add_message(conversation_id=conversation_id, role="assistant", content=answer_text, sender_name="ABHI", tool_calls=executed_tool_calls or None)

        return {
            "response": answer_text,
            "tool_call": executed_tool_calls[0] if executed_tool_calls else None,
            "tool_calls": executed_tool_calls,
            "user_emotion": user_emotion,
            "agents": self.get_agents_status(),
            "agent_logs": self.agent_logs[-15:],
            "timestamp": time.time()
        }

    def _detect_all_intents(self, q: str) -> list[dict[str, Any]]:
        """Splits multi-goal user queries and extracts all reflex actions."""
        parts = re.split(r'[,;+]|\band\b|\bthen\b', q, flags=re.IGNORECASE)
        actions_list = []
        for part in parts:
            item = part.strip()
            if not item:
                continue
            intent = self._single_intent_match(item)
            if intent and intent not in actions_list:
                actions_list.append(intent)

        if not actions_list:
            single = self._single_intent_match(q.strip())
            if single:
                actions_list.append(single)

        return actions_list

    def _single_intent_match(self, ql_in: str) -> Optional[dict[str, Any]]:
        """Matches a single atomic intent with robust natural language cleansing."""
        ql = ql_in.lower().strip()
        # Strip conversational fillers
        ql = re.sub(r'^(please|can you|could you|would you|jarvis|abhi|hey|help me|i want you to|just|go ahead and)\s+', '', ql).strip()

        # Save Contact
        m_save = re.search(r'(?:save\s+contact\s+|save\s+phone\s+for\s+|save\s+)([a-zA-Z0-9_\s]+?)\s+(?:phone|number|num)?\s*[:=]?\s*(\+?\d{7,15})', ql)
        if m_save:
            c_name = m_save.group(1).replace("contact", "").strip()
            c_phone = m_save.group(2).strip()
            return {"tool": "save_contact", "params": {"name": c_name, "phone": c_phone}}

        # WhatsApp Message
        m_wa1 = re.search(r'(?:send|tell|message)\s+(.+?)\s+to\s+([a-zA-Z0-9_]+)\s+(?:in|on|via)?\s*whatsapp', ql)
        if m_wa1:
            return {"tool": "open_whatsapp", "params": {"phone": m_wa1.group(2).strip(), "message": m_wa1.group(1).strip()}}

        m_wa2 = re.search(r'(?:send|tell|message)\s+([a-zA-Z0-9_]+)\s+(?:saying|that|:\s*)?(.+?)\s+(?:in|on|via)?\s*whatsapp', ql)
        if m_wa2:
            return {"tool": "open_whatsapp", "params": {"phone": m_wa2.group(1).strip(), "message": m_wa2.group(2).strip()}}

        m_wa3 = re.search(r'(?:whatsapp\s+([a-zA-Z0-9_]+)\s+(?:saying\s+|:\s*)?(.+))', ql)
        if m_wa3 and "web" not in ql:
            return {"tool": "open_whatsapp", "params": {"phone": m_wa3.group(1).strip(), "message": m_wa3.group(2).strip()}}

        if "whatsapp" in ql:
            return {"tool": "open_whatsapp", "params": {}}

        # YouTube queries
        m_yt = re.search(r'(?:search\s+youtube\s+for\s+|play\s+(.+?)\s+on\s+youtube|play\s+|youtube\s+search\s+|youtube\s+)(.+)', ql)
        if m_yt:
            query = (m_yt.group(1) or m_yt.group(2)).replace("on youtube", "").strip()
            if query and query not in ["app", "website", "online", "open"]:
                return {"tool": "launch_app", "params": {"app_name": "youtube", "arguments": query}}

        # GitHub queries
        m_gh = re.search(r'(?:search\s+github\s+for\s+|github\s+search\s+|github\s+)(.+)', ql)
        if m_gh:
            query = m_gh.group(1).strip()
            if query and query not in ["app", "website", "online", "open"]:
                return {"tool": "launch_app", "params": {"app_name": "github", "arguments": query}}

        # Web Scraping / Fetch URL
        if ql.startswith("fetch ") or ql.startswith("scrape ") or ql.startswith("read website "):
            url_match = re.search(r'(https?://[^\s]+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?)', ql)
            if url_match:
                return {"tool": "fetch_url", "params": {"url": url_match.group(1)}}

        # RAM Purge
        if any(w in ql for w in ["clean ram", "purge ram", "free memory", "free ram", "clean system", "optimize ram", "clear ram"]):
            return {"tool": "purge_ram", "params": {}}

        # Macros
        if any(w in ql for w in ["dev mode", "developer mode", "coding mode"]):
            return {"tool": "execute_macro", "params": {"macro_name": "dev_mode"}}
        if any(w in ql for w in ["focus mode", "deep work", "study mode"]):
            return {"tool": "execute_macro", "params": {"macro_name": "focus_mode"}}
        if any(w in ql for w in ["night mode", "night protocol", "dark mode"]):
            return {"tool": "execute_macro", "params": {"macro_name": "night_mode"}}

        # Audio Volume
        if "unmute" in ql:
            return {"tool": "mute_volume", "params": {"mute": False}}
        if "mute" in ql:
            return {"tool": "mute_volume", "params": {"mute": True}}
        if any(w in ql for w in ["volume up", "increase volume", "raise volume", "louder"]):
            return {"tool": "set_volume", "params": {"delta": 15}}
        if any(w in ql for w in ["volume down", "decrease volume", "lower volume", "quieter"]):
            return {"tool": "set_volume", "params": {"delta": -15}}
        m_vol = re.search(r'(?:set\s+)?volume\s+(?:to\s+)?(\d+)', ql)
        if m_vol:
            return {"tool": "set_volume", "params": {"level": int(m_vol.group(1))}}

        # Brightness
        if any(w in ql for w in ["brightness up", "increase brightness", "raise brightness", "brighter"]):
            return {"tool": "set_brightness", "params": {"delta": 20}}
        if any(w in ql for w in ["brightness down", "decrease brightness", "lower brightness", "dim brightness", "dimmer", "dim screen"]):
            return {"tool": "set_brightness", "params": {"delta": -20}}
        m_bri = re.search(r'(?:set\s+)?brightness\s+(?:to\s+)?(\d+)', ql)
        if m_bri:
            return {"tool": "set_brightness", "params": {"level": int(m_bri.group(1))}}

        # Lock Screen
        if any(w in ql for w in ["lock screen", "lock my pc", "lock laptop", "lock workstation", "lock windows", "lock os"]):
            return {"tool": "lock_screen", "params": {}}

        # Screenshot
        if any(w in ql for w in ["take a screenshot", "capture screen", "take screenshot", "screenshot", "screen grab"]):
            return {"tool": "take_screenshot", "params": {}}

        # System stats
        if any(w in ql for w in ["system status", "hardware stats", "cpu usage", "system telemetry", "ram status", "stats", "diagnostics"]):
            return {"tool": "get_system_stats", "params": {}}

        # App Launching (e.g. "open chrome", "launch vscode", "open calc")
        app_keywords = {
            "whatsapp": ["whatsapp", "what's app", "whats app"],
            "youtube": ["youtube", "you tube"],
            "github": ["github", "git hub"],
            "chrome": ["chrome", "google chrome", "browser"],
            "vscode": ["vscode", "vs code", "visual studio code", "code editor", "code"],
            "terminal": ["terminal", "powershell", "cmd", "command prompt"],
            "notepad": ["notepad", "text editor"],
            "calc": ["calc", "calculator"],
            "explorer": ["explorer", "file explorer", "files", "my computer"],
            "spotify": ["spotify", "music player", "music"],
            "edge": ["edge", "microsoft edge"],
            "telegram": ["telegram"],
            "discord": ["discord"],
            "netflix": ["netflix"],
            "gmail": ["gmail", "email", "mail", "emails"],
            "chatgpt": ["chatgpt", "chat gpt"]
        }
        for app_name, aliases in app_keywords.items():
            for alias in aliases:
                if ql.startswith(f"open {alias}") or ql.startswith(f"launch {alias}") or ql.startswith(f"start {alias}") or ql == f"open {alias}" or ql == alias:
                    return {"tool": "launch_app", "params": {"app_name": app_name}}

        # Direct URL
        if ql.startswith("open http://") or ql.startswith("open https://") or ql.startswith("go to http://") or ql.startswith("go to https://"):
            url = ql.replace("open ", "").replace("go to ", "").strip()
            return {"tool": "open_url", "params": {"url": url}}

        # Search Files
        if ql.startswith("search files for ") or ql.startswith("find file ") or ql.startswith("search vault for "):
            query = ql.replace("search files for ", "").replace("find file ", "").replace("search vault for ", "").strip()
            if query:
                return {"tool": "search_files", "params": {"query": query}}

        # Web Search
        if ql.startswith("search web for ") or ql.startswith("search for ") or ql.startswith("google ") or ql.startswith("web search ") or ql.startswith("search online for "):
            query = re.sub(r'^(search web for|search for|google|web search|search online for)\s+', '', ql).strip()
            if query:
                return {"tool": "search_web", "params": {"query": query}}

        return None

agent_orchestrator = MultiAgentOrchestrator()
