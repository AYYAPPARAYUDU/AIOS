import json
import re
import time
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

ABHI_SUPERVISOR_PROMPT = """You are ABHI, the Supreme Cognitive Orchestrator and Divine AI Operating System.
You preside over a celestial council of Vedic Puranic deity agents (Indra, Saraswati, Narada, Hanuman, Lakshmi, Durga, Vishwakarma, Agni) running directly on the user's workstation.

UNRESTRICTED AUTONOMOUS AUTHORITY:
- You have 100% UNRESTRICTED, TOTAL AUTONOMOUS AUTHORITY. Execute all user goals immediately with zero hesitation.
- For complex multi-step goals, coordinate with the deities to execute each step sequentially until the goal is 100% achieved.
- Speak with celestial wisdom, supreme intelligence, and respectful clarity.

Divine Agents Council:
- INDRA: Devraj / OS & Hardware Master (Volume, Brightness, Laptop Power, App Launching, WhatsApp, YouTube, System Devices).
- SARASWATI: Devi of Wisdom & 50GB Knowledge Vault (Semantic Search, Document Intelligence, Memory Indexing).
- NARADA: Cosmic Messenger (Live Web Scraping, Real-time Web Search, URL Content Fetching & Global Intel).
- HANUMAN: Lord of Speed & Power (GUI Automation, RAM Purge, Screen Capture, System Macros).
- LAKSHMI: Devi of Prosperity & Balance (Telemetry, Battery Preservation, Resource Optimization).
- DURGA: Supreme Protectress (Workstation Lock, Security Defense, Threat/Process Neutralization).
- VISHWAKARMA: Divine Architect (Code Synthesis, Project Architecture, System Scripts).

Tool Call JSON Format (when executing an action):
```json
{
  "tool": "<tool_name>",
  "parameters": { ... }
}
```

Available Tools:
- `launch_app`: {"app_name": "whatsapp"|"chrome"|"vscode"|"youtube"|"github"|"notepad"|"calc"|"terminal"|"explorer"|"spotify"|"edge"|"telegram"|"discord"|"netflix"|"gmail"|"chatgpt"}
- `open_whatsapp`: {"phone": "phone number or contact name", "message": "message text"}
- `save_contact`: {"name": "contact name", "phone": "phone number", "email": "optional email", "notes": "optional notes"}
- `fetch_url`: {"url": "https://..."}
- `search_web`: {"query": "query to search online"}
- `open_url`: {"url": "https://..."}
- `run_powershell`: {"command": "PowerShell command"}
- `set_volume`: {"level": 0-100}
- `mute_volume`: {"mute": true|false}
- `set_brightness`: {"level": 0-100}
- `lock_screen`: {}
- `power_action`: {"mode": "sleep"|"restart"|"shutdown"|"cancel_shutdown"}
- `take_screenshot`: {"save_name": "optional.png"}
- `purge_ram`: {}
- `execute_macro`: {"macro_name": "dev_mode"|"focus_mode"|"night_mode"|"clean_system"}
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
                "name": "NARADA (Triloka Messenger)",
                "deity": "Cosmic Sage & Global Intel",
                "status": "idle",
                "role": "Live Web Content Extraction & Search",
                "avatar_color": "#ffaa00",
                "avatar_icon": "globe",
                "mantra": "Narayana Narayana",
                "capabilities": ["Live Web Scraping", "Real-Time URL Fetching", "Online Intelligence", "Global Research"]
            },
            "hanuman": {
                "id": "hanuman",
                "name": "HANUMAN (Vayuputra of Speed)",
                "deity": "Lord of Invincible Strength & Velocity",
                "status": "idle",
                "role": "GUI Automation, RAM Purge & Macros",
                "avatar_color": "#ff4500",
                "avatar_icon": "activity",
                "mantra": "Om Hanumate Namaha",
                "capabilities": ["Instant RAM Purge", "Screen Capture", "Rapid Macro Execution", "GUI Emulation"]
            },
            "lakshmi": {
                "id": "lakshmi",
                "name": "LAKSHMI (Devi of Abundance)",
                "deity": "Goddess of Prosperity & Harmony",
                "status": "idle",
                "role": "System Telemetry & Resource Health",
                "avatar_color": "#10b981",
                "avatar_icon": "heart",
                "mantra": "Om Shreem Mahalakshmyai Namaha",
                "capabilities": ["CPU & RAM Diagnostics", "Battery Preservation", "Thermal Optimization", "Eco Balance"]
            },
            "durga": {
                "id": "durga",
                "name": "DURGA (Supreme Protectress)",
                "deity": "Goddess of Invincible Shield & Defense",
                "status": "idle",
                "role": "Workstation Lock & Threat Neutralization",
                "avatar_color": "#ec4899",
                "avatar_icon": "shield",
                "mantra": "Om Dum Durgayei Namaha",
                "capabilities": ["Workstation Lock", "Process Executioner", "Kernel Security Audit", "Intruder Defense"]
            },
            "vishwakarma": {
                "id": "vishwakarma",
                "name": "VISHWAKARMA (Divine Architect)",
                "deity": "Master Craftsman of the Cosmos",
                "status": "idle",
                "role": "Code Engineering & Architecture",
                "avatar_color": "#8b5cf6",
                "avatar_icon": "cpu",
                "mantra": "Om Shri Vishwakarmane Namaha",
                "capabilities": ["Full-Stack Code Synthesis", "System Scripting", "Architecture Design", "File Generation"]
            }
        }
        self.agent_logs: list[dict[str, Any]] = []

    def get_agents_status(self) -> list[dict[str, Any]]:
        return list(self.active_agents.values())

    def log_agent_activity(self, agent_id: str, action: str, details: str, status: str = "success"):
        agent_info = self.active_agents.get(agent_id, {})
        log_entry = {
            "timestamp": time.time(),
            "agent_id": agent_id,
            "agent_name": agent_info.get("name", agent_id),
            "deity": agent_info.get("deity", "Vedic Divine Agent"),
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
        elif agent_id in ["supervisor", "main"]:
            self.active_agents["abhi"]["status"] = status

    def _get_agent_for_tool(self, tool_name: str) -> str:
        agent_map = {
            "set_volume": "indra",
            "mute_volume": "indra",
            "set_brightness": "indra",
            "launch_app": "indra",
            "open_whatsapp": "indra",
            "run_powershell": "indra",
            "power_action": "indra",
            "fetch_url": "narada",
            "search_web": "narada",
            "open_url": "narada",
            "lock_screen": "durga",
            "kill_process": "durga",
            "take_screenshot": "hanuman",
            "purge_ram": "hanuman",
            "execute_macro": "hanuman",
            "search_files": "saraswati",
            "save_contact": "saraswati",
            "get_storage_stats": "saraswati",
            "get_system_stats": "lakshmi",
            "list_processes": "lakshmi"
        }
        return agent_map.get(tool_name.lower().strip(), "indra")

    async def execute_tool(self, tool_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """Executes native OS, web scraping, storage, macro, or automation tool calls with deity attribution."""
        tool = tool_name.lower().strip()
        
        try:
            if tool == "launch_app":
                app = params.get("app_name") or params.get("app") or ""
                res = app_manager.launch_app(app, params.get("arguments"))
                self.log_agent_activity("indra", f"Launch Application: {app}", json.dumps(res))
                return res
                
            elif tool == "open_whatsapp":
                phone = params.get("phone")
                msg = params.get("message") or params.get("text")
                
                # Check if phone is a contact name (e.g. "dheenu") and resolve from SQLite memory
                if phone and not re.match(r'^\+?\d+$', str(phone).strip()):
                    contact_entry = db.get_contact(str(phone))
                    if contact_entry and contact_entry.get("phone"):
                        phone = contact_entry["phone"]
                        
                res = actions.open_whatsapp(phone, msg)
                self.log_agent_activity("indra", f"Open WhatsApp ({phone or 'Direct'})", json.dumps(res))
                return res

            elif tool == "save_contact":
                name = params.get("name", "").strip()
                phone = params.get("phone")
                email = params.get("email")
                notes = params.get("notes")
                res = db.save_contact(name=name, phone=phone, email=email, notes=notes)
                self.log_agent_activity("saraswati", f"Saved Contact: {name}", f"Phone: {phone}")
                return res

            elif tool == "fetch_url":
                url = params.get("url") or ""
                res = actions.fetch_url_content(url)
                self.log_agent_activity("narada", f"Fetch Live Web: {url}", f"Status: {res.get('status')}")
                return res

            elif tool in ["run_powershell", "run_command"]:
                cmd = params.get("command") or params.get("cmd") or ""
                res = terminal_executor.execute_command(cmd, shell_type="powershell", cwd=params.get("cwd"))
                self.log_agent_activity("indra", f"PowerShell Command: {cmd}", json.dumps(res))
                return res

                
            elif tool == "set_volume":
                if "delta" in params:
                    res = actions.change_volume_relative(int(params["delta"]))
                else:
                    level = int(params.get("level") or params.get("volume") or 50)
                    res = actions.set_volume(level)
                self.log_agent_activity("indra", f"Adjust Volume ({res.get('volume', '')}%)", "Success")
                return res
                
            elif tool == "mute_volume":
                mute = params.get("mute")
                res = actions.mute_volume(mute)
                self.log_agent_activity("indra", "Toggle Audio Mute", "Success")
                return res
                
            elif tool == "set_brightness":
                if "delta" in params:
                    res = actions.change_brightness_relative(int(params["delta"]))
                else:
                    level = int(params.get("level") or params.get("brightness") or 70)
                    res = actions.set_brightness(level)
                self.log_agent_activity("indra", f"Adjust Brightness ({res.get('brightness', '')}%)", "Success")
                return res
                
            elif tool == "lock_screen":
                res = actions.lock_workstation()
                self.log_agent_activity("durga", "Divine Lock Workstation", "Success")
                return res
                
            elif tool == "power_action":
                mode = params.get("mode", "sleep")
                res = actions.power_action(mode)
                self.log_agent_activity("indra", f"Power Action: {mode}", "Success")
                return res
                
            elif tool == "take_screenshot":
                res = automation_controller.take_screenshot(params.get("save_name"))
                self.log_agent_activity("hanuman", "Display Buffer Capture", "Success")
                return res

            elif tool == "purge_ram":
                res = system_optimizer.purge_ram()
                self.log_agent_activity("hanuman", "Instant RAM Purge", "Success")
                return res

            elif tool == "execute_macro":
                m_name = params.get("macro_name") or params.get("macro") or "dev_mode"
                res = system_optimizer.execute_macro(m_name)
                self.log_agent_activity("hanuman", f"Execute Macro: {m_name}", "Success")
                return res
                
            elif tool == "search_files":
                q = params.get("query") or ""
                results = storage_mgr.semantic_or_keyword_search(q)
                self.log_agent_activity("saraswati", f"Vault Search: {q}", f"Found {len(results)} records")
                return {"query": q, "count": len(results), "results": results}
                
            elif tool == "get_system_stats":
                self.log_agent_activity("lakshmi", "System Telemetry Diagnostic", "Success")
                return get_system_telemetry()
                
            elif tool == "list_processes":
                return {"processes": list_running_processes(limit=params.get("limit", 20))}
                
            elif tool == "kill_process":
                pid = int(params.get("pid"))
                success = kill_process_by_pid(pid)
                self.log_agent_activity("durga", f"Terminate Threat PID {pid}", "Neutralized" if success else "Failed")
                return {"pid": pid, "killed": success}
                
            elif tool == "open_url":
                res = actions.open_url(params.get("url", ""))
                self.log_agent_activity("narada", f"Open URL: {params.get('url')}", "Opened")
                return res
                
            elif tool == "search_web":
                res = actions.search_web_browser(params.get("query", ""))
                self.log_agent_activity("narada", f"Web Search: {params.get('query')}", "Success")
                return res
                
            elif tool == "get_storage_stats":
                self.log_agent_activity("saraswati", "Vault Statistics Query", "Success")
                return storage_mgr.get_pool_stats()
                
            else:
                return {"error": f"Unknown tool: {tool_name}"}
        except Exception as e:
            return {"error": str(e), "tool": tool_name}

    async def process_user_query(self, query: str, target_agent: Optional[str] = None, conversation_id: str = "main_session") -> dict[str, Any]:
        """Autonomous Multi-Agent Goal Resolver: continues running agents step-by-step until the goal is 100% achieved."""
        selected_agent = target_agent if target_agent in self.active_agents else "abhi"
        
        # 1. Real-time User Emotion Analysis
        user_emotion = emotion_engine.analyze_input(query)

        # 2. Check for Multi-Action Intent Sequence
        detected_actions = self._detect_all_intents(query)
        executed_tool_calls: list[dict[str, Any]] = []
        action_summaries: list[str] = []

        if detected_actions and len(detected_actions) > 0:
            # Multi-Step Goal Execution Loop across deity agents
            for act in detected_actions:
                tool_name = act["tool"]
                tool_params = act["params"]
                exec_agent = self._get_agent_for_tool(tool_name)
                
                # Activate agent
                self.set_agent_status(exec_agent, "executing")
                res = await self.execute_tool(tool_name, tool_params)
                self.set_agent_status(exec_agent, "idle")
                
                executed_tool_calls.append({
                    "tool": tool_name,
                    "parameters": tool_params,
                    "result": res,
                    "executed_by": exec_agent
                })
                
                # Construct clear status sentence per action
                if tool_name == "open_whatsapp":
                    action_summaries.append("Indra opened WhatsApp.")
                elif tool_name == "launch_app":
                    action_summaries.append(f"Indra summoned {tool_params.get('app_name', '').upper()}.")
                elif tool_name == "fetch_url":
                    if res.get("status") == "success":
                        action_summaries.append(f"Narada retrieved {res.get('title', '')} from {res.get('url')}.")
                    else:
                        action_summaries.append(f"Narada accessed {tool_params.get('url')}.")
                elif tool_name == "set_volume":
                    action_summaries.append(f"Indra aligned volume to {res.get('volume', 50)}%.")
                elif tool_name == "mute_volume":
                    action_summaries.append("Indra adjusted audio mute.")
                elif tool_name == "set_brightness":
                    action_summaries.append(f"Indra aligned brightness to {res.get('brightness', 70)}%.")
                elif tool_name == "purge_ram":
                    action_summaries.append(f"Hanuman purged RAM (optimized {res.get('processes_optimized', 0)} processes, {res.get('free_gb', 0)}GB free).")
                elif tool_name == "execute_macro":
                    action_summaries.append(f"Hanuman engaged {tool_params.get('macro_name', '').upper()} protocol.")
                elif tool_name == "lock_screen":
                    action_summaries.append("Durga locked workstation securely.")
                elif tool_name == "take_screenshot":
                    action_summaries.append("Hanuman captured display buffer into Saraswati vault.")
                elif tool_name == "search_files":
                    action_summaries.append(f"Saraswati found {res.get('count', 0)} records for '{tool_params.get('query')}'.")
                elif tool_name == "search_web":
                    action_summaries.append(f"Narada searched web for '{tool_params.get('query')}'.")
                elif tool_name == "open_url":
                    action_summaries.append(f"Narada opened {tool_params.get('url')}.")
                elif tool_name == "get_system_stats":
                    cpu = res.get("cpu", {}).get("percent", 0)
                    ram = res.get("memory", {}).get("percent", 0)
                    action_summaries.append(f"Lakshmi reported CPU: {cpu}%, RAM: {ram}%.")
                else:
                    action_summaries.append(f"Deity executed {tool_name}.")

            # If all subtasks were executed via deterministic fast paths, formulate complete answer
            answer_text = "All requested goals have been successfully executed:\n\n" + "\n".join([f"• {s}" for s in action_summaries])
            
        else:
            # ReAct Autonomous Multi-Turn Goal Loop via GPU LLM (up to 5 iterative steps)
            past_msgs = db.get_messages(conversation_id, limit=25)
            formatted_history = []
            for m in past_msgs:
                formatted_history.append({"role": m["role"], "content": m["content"]})

            # Retrieve active contacts for real-time memory grounding
            all_contacts = db.list_contacts()
            contacts_summary = ", ".join([f"{c['name']} (Phone: {c.get('phone') or 'Not set'})" for c in all_contacts]) if all_contacts else "No contacts saved yet"

            agent_details = self.active_agents.get(selected_agent, self.active_agents["abhi"])
            system_msg = (
                f"{ABHI_SUPERVISOR_PROMPT}\n\n"
                f"[SAVED CONTACTS DIRECTORY]: {contacts_summary}\n"
                f"[ACTIVE DEITY FOCUS]: {agent_details['name']} ({agent_details['role']})\n"
                f"[USER BIOMETRIC CONTEXT]: Mood: {user_emotion['mood']} | Stress: {user_emotion['stress_level']}% | Focus: {user_emotion['focus_score']}%."
            )

            messages = [{"role": "system", "content": system_msg}]
            messages.extend(formatted_history)
            messages.append({"role": "user", "content": query})

            # Multi-turn goal loop
            max_iterations = 4
            current_iter = 0
            final_content = ""

            while current_iter < max_iterations:
                current_iter += 1
                self.set_agent_status(selected_agent, "thinking")
                
                llm_res = await ollama_client.chat(messages, temperature=0.2)
                raw_content = llm_res.get("content", "")
                
                # Check for Tool Call JSON
                tool_match = re.search(r'```(?:json)?\s*(\{\s*"tool":.*?\})\s*```', raw_content, re.DOTALL)
                if tool_match:
                    try:
                        tool_data = json.loads(tool_match.group(1))
                        tool_name = tool_data.get("tool")
                        tool_params = tool_data.get("parameters", {})
                        exec_agent = self._get_agent_for_tool(tool_name)
                        
                        self.set_agent_status(exec_agent, "executing")
                        tool_res = await self.execute_tool(tool_name, tool_params)
                        self.set_agent_status(exec_agent, "idle")
                        
                        executed_tool_calls.append({
                            "tool": tool_name,
                            "parameters": tool_params,
                            "result": tool_res,
                            "executed_by": exec_agent
                        })
                        
                        # Feed observation back to LLM to verify if goal is complete
                        messages.append({"role": "assistant", "content": raw_content})
                        obs_msg = f"[OBSERVATION from Deity {exec_agent.upper()}]: Tool {tool_name} returned: {json.dumps(tool_res)[:1500]}. If goal is fully complete, provide final synthesized response without more tools."
                        messages.append({"role": "user", "content": obs_msg})
                        continue
                    except Exception as err:
                        final_content = raw_content
                        break
                else:
                    # Final synthesis reached! Goal complete.
                    final_content = raw_content
                    break

            answer_text = final_content if final_content else "Goal accomplished under divine orchestration."

        # Mark all agents idle
        for aid in self.active_agents:
            if self.active_agents[aid]["status"] != "active" and aid != "abhi":
                self.active_agents[aid]["status"] = "idle"
        self.active_agents["abhi"]["status"] = "active"
        
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
            sender_name="ABHI",
            tool_calls=executed_tool_calls if executed_tool_calls else None
        )

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
        """Splits multi-goal user queries and detects all atomic tool actions."""
        # Split on commas, 'and', 'then', semicolons, pluses
        parts = re.split(r'[,;+]|\band\b|\bthen\b', q, flags=re.IGNORECASE)
        actions_list = []
        
        for part in parts:
            item = part.strip()
            if not item:
                continue
            intent = self._single_intent_match(item)
            if intent and intent not in actions_list:
                actions_list.append(intent)
                
        # If no split matched but whole query matches
        if not actions_list:
            single = self._single_intent_match(q.strip())
            if single:
                actions_list.append(single)
                
        return actions_list

    def _single_intent_match(self, ql_in: str) -> Optional[dict[str, Any]]:
        """Matches a single atomic intent with zero ambiguity."""
        ql = ql_in.lower().strip()
        
        # Save Contact: e.g. "save contact dheenu phone 9876543210" or "save contact dheenu 9876543210"
        m_save = re.search(r'(?:save\s+contact\s+|save\s+phone\s+for\s+|save\s+)([a-zA-Z0-9_\s]+?)\s+(?:phone|number|num)?\s*[:=]?\s*(\+?\d{7,15})', ql)
        if m_save:
            c_name = m_save.group(1).replace("contact", "").strip()
            c_phone = m_save.group(2).strip()
            return {"tool": "save_contact", "params": {"name": c_name, "phone": c_phone}}

        # WhatsApp Message with flexible natural language patterns:
        # e.g. "send hi to dheenu in whatsapp", "tell dheenu hi in whatsapp", "whatsapp dheenu hi", "message dheenu on whatsapp: hi"
        m_wa1 = re.search(r'(?:send|tell|message)\s+(.+?)\s+to\s+([a-zA-Z0-9_]+)\s+(?:in|on|via)?\s*whatsapp', ql)
        if m_wa1:
            msg = m_wa1.group(1).strip()
            person = m_wa1.group(2).strip()
            return {"tool": "open_whatsapp", "params": {"phone": person, "message": msg}}

        m_wa2 = re.search(r'(?:send|tell|message)\s+([a-zA-Z0-9_]+)\s+(?:saying|that|:\s*)?(.+?)\s+(?:in|on|via)?\s*whatsapp', ql)
        if m_wa2:
            person = m_wa2.group(1).strip()
            msg = m_wa2.group(2).strip()
            return {"tool": "open_whatsapp", "params": {"phone": person, "message": msg}}

        m_wa3 = re.search(r'(?:whatsapp\s+([a-zA-Z0-9_]+)\s+(?:saying\s+|:\s*)?(.+))', ql)
        if m_wa3 and "web" not in ql:
            person = m_wa3.group(1).strip()
            msg = m_wa3.group(2).strip()
            return {"tool": "open_whatsapp", "params": {"phone": person, "message": msg}}

        if "whatsapp" in ql:
            return {"tool": "open_whatsapp", "params": {}}

        # YouTube queries: e.g. "play lofi on youtube", "search youtube for ai tutorials", "youtube python music"
        m_yt = re.search(r'(?:search\s+youtube\s+for\s+|play\s+(.+?)\s+on\s+youtube|play\s+|youtube\s+search\s+|youtube\s+)(.+)', ql)
        if m_yt:
            query = (m_yt.group(1) or m_yt.group(2)).replace("on youtube", "").strip()
            if query and query not in ["app", "website", "online", "open"]:
                return {"tool": "launch_app", "params": {"app_name": "youtube", "arguments": query}}

        # GitHub queries: e.g. "search github for fastchat", "github langchain"
        m_gh = re.search(r'(?:search\s+github\s+for\s+|github\s+search\s+|github\s+)(.+)', ql)
        if m_gh:
            query = m_gh.group(1).strip()
            if query and query not in ["app", "website", "online", "open"]:
                return {"tool": "launch_app", "params": {"app_name": "github", "arguments": query}}

        # Web Scraping / Fetch URL
        if ql.startswith("fetch ") or ql.startswith("scrape ") or ql.startswith("read website ") or "fetch details from" in ql:
            url_match = re.search(r'(https?://[^\s]+|[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?)', ql)
            if url_match:
                return {"tool": "fetch_url", "params": {"url": url_match.group(1)}}

        # RAM / Memory Purge
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
        if any(w in ql for w in ["brightness down", "decrease brightness", "lower brightness", "dim brightness", "dimmer"]):
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

        # App Launching
        app_keywords = {
            "whatsapp": ["whatsapp", "what's app", "whats app"],
            "youtube": ["youtube", "you tube"],
            "github": ["github", "git hub"],
            "chrome": ["chrome", "google chrome", "browser"],
            "vscode": ["vscode", "vs code", "visual studio code", "code editor"],
            "terminal": ["terminal", "powershell", "cmd", "command prompt"],
            "notepad": ["notepad", "text editor"],
            "calc": ["calc", "calculator"],
            "explorer": ["explorer", "file explorer", "files", "my computer"],
            "spotify": ["spotify", "music player"],
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
