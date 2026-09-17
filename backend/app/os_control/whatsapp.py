import os
import re
import time
import urllib.parse
import subprocess
import ctypes
import logging
from typing import Optional, Any
from backend.app.storage.db import db

logger = logging.getLogger(__name__)

# Win32 Constants
SW_RESTORE = 9
SW_SHOW = 5

class WhatsAppController:
    """Natively controls WhatsApp Desktop app and Web on the OS with 100% automation."""

    @staticmethod
    def _is_windows() -> bool:
        return os.name == 'nt'

    @classmethod
    def focus_whatsapp_window(cls) -> bool:
        """Finds and brings WhatsApp Desktop window to the foreground on Windows."""
        if not cls._is_windows():
            return False

        try:
            user32 = ctypes.windll.user32
            found_hwnds = []

            def enum_windows_callback(hwnd, extra):
                if user32.IsWindowVisible(hwnd):
                    length = user32.GetWindowTextLengthW(hwnd)
                    if length > 0:
                        buff = ctypes.create_unicode_buffer(length + 1)
                        user32.GetWindowTextW(hwnd, buff, length + 1)
                        title = buff.value
                        if "whatsapp" in title.lower():
                            found_hwnds.append(hwnd)
                return True

            WNDENUMPROC = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_int, ctypes.c_int)
            user32.EnumWindows(WNDENUMPROC(enum_windows_callback), 0)

            if found_hwnds:
                target_hwnd = found_hwnds[0]
                user32.ShowWindow(target_hwnd, SW_RESTORE)
                user32.SetForegroundWindow(target_hwnd)
                time.sleep(0.3)
                return True
        except Exception as e:
            logger.warning(f"Error focusing WhatsApp window: {e}")
        return False

    @classmethod
    def launch_whatsapp(cls) -> dict[str, Any]:
        """Launches WhatsApp application via native URI, Windows Store shell app, or Web."""
        launched = False
        method = "none"

        if cls._is_windows():
            # 1. Try WhatsApp URI protocol
            try:
                os.startfile("whatsapp:")
                launched = True
                method = "windows_uri"
            except Exception:
                pass

            # 2. Try Windows Store AppsFolder Package ID
            if not launched:
                try:
                    subprocess.Popen(["explorer.exe", "shell:AppsFolder\\5319275A.WhatsAppDesktop_cv1g1gvanyjgm!App"])
                    launched = True
                    method = "windows_store_app"
                except Exception:
                    pass

            # 3. Try standard executable paths
            if not launched:
                exe_paths = [
                    os.path.expandvars(r"%LOCALAPPDATA%\WhatsApp\WhatsApp.exe"),
                    os.path.expandvars(r"%LOCALAPPDATA%\Programs\WhatsApp\WhatsApp.exe"),
                    r"C:\Program Files\WhatsApp\WhatsApp.exe"
                ]
                for path in exe_paths:
                    if os.path.exists(path):
                        try:
                            subprocess.Popen([path])
                            launched = True
                            method = "executable"
                            break
                        except Exception:
                            continue

        # 4. Fallback: WhatsApp Web in default browser
        if not launched:
            from backend.app.os_control.apps import _open_url_robust
            _open_url_robust("https://web.whatsapp.com")
            launched = True
            method = "web_fallback"

        db.log_audit("OS_WHATSAPP", f"Launch WhatsApp via {method}", "INDRA_Controller", "SUCCESS" if launched else "FAILED")
        return {"status": "success" if launched else "failed", "method": method}

    @classmethod
    def generate_smart_context(
        cls,
        prompt_or_reason: Optional[str] = None,
        recipient: Optional[str] = None,
        context_type: str = "auto"
    ) -> str:
        """Universal AI Context Generator: Uses local 8B LLM for ALL scenarios with rich template fallbacks."""
        raw = (prompt_or_reason or "").strip()
        recipient_display = recipient.capitalize() if recipient and not re.search(r'\d{6,}', recipient) else "Sir/Madam"

        if not raw:
            return f"Hello {recipient_display}, hope you are doing well."

        # 1. Try Local 8B LLM (Ollama) if available
        try:
            from backend.app.core.llm import ollama_client
            import asyncio

            async def _call_llm():
                system_prompt = (
                    "You are JARVIS AIOS, drafting a concise, natural, highly professional WhatsApp message. "
                    "Draft ONLY the message text without quotes, conversational prefixes, or meta-explanations. "
                    f"Recipient: {recipient_display}. Sender: Ayyappa Rayudu."
                )
                msgs = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Draft WhatsApp message for this request/scenario: {raw}"}
                ]
                res = await ollama_client.chat(msgs, temperature=0.3)
                content = res.get("content", "").strip()
                if content and not content.startswith("Ollama Error"):
                    return content
                return None

            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # If in running loop, run in background or fallback to heuristic
                    pass
                else:
                    llm_text = loop.run_until_complete(_call_llm())
                    if llm_text:
                        return llm_text
            except Exception:
                pass
        except Exception:
            pass

        raw_lower = raw.lower()

        # 2. Scenario A: Leave Letter / Medical / Sickness / Time-off
        is_leave = bool(re.search(r'\b(leave|fever|sick|hospital|emergency|vacation|wedding|medical|headache|stomach|unwell|time\s*off)\b', raw_lower))
        if is_leave or context_type == "leave_letter":
            reason_clean = raw
            for prefix in [
                r'.*?\bleave\s+(?:letter|note|application|request)\s+(?:on|for|due\s+to|because\s+of|with\s+reason)?\s*',
                r'.*?\breason\s*[:=]?\s*',
                r'.*?\bbecause\s+of\s*',
                r'.*?\bdue\s+to\s*'
            ]:
                match = re.search(prefix, reason_clean, re.IGNORECASE)
                if match:
                    extracted = reason_clean[match.end():].strip()
                    if extracted and len(extracted) > 2:
                        reason_clean = extracted
                        break

            reason_clean = re.sub(r'^(?:this\s+reason|fever\s+reason)\b', 'severe fever and unwellness', reason_clean, flags=re.IGNORECASE).strip()
            if not reason_clean or reason_clean.lower() in ["this reason", "reason", "leave letter", "fever reason", "leave"]:
                reason_clean = "health indisposition and medical rest"

            return (
                f"Subject: Leave Application - {reason_clean.capitalize()}\n\n"
                f"Dear {recipient_display},\n\n"
                f"I am writing to formally request a leave of absence due to {reason_clean}. "
                f"I am unable to attend to my regular duties today. I will ensure all urgent matters are prioritized upon my return, "
                f"and I remain available on WhatsApp/phone for any critical emergencies.\n\n"
                f"Kindly grant me leave for the day.\n\n"
                f"Thank you for your understanding.\n\n"
                f"Warm regards,\n"
                f"Ayyappa Rayudu"
            )

        # 3. Scenario B: Project / Task / Sprint Update
        is_update = bool(re.search(r'\b(project|update|task|status|deployment|release|sprint|done|completed|progress)\b', raw_lower))
        if is_update:
            clean_info = re.sub(r'^(?:project\s+update|update|status|about|that)\s*[:=]?\s*', '', raw, flags=re.IGNORECASE).strip()
            return (
                f"Hi {recipient_display},\n\n"
                f"Quick status update regarding our progress:\n"
                f"• {clean_info.capitalize()}\n\n"
                f"All core components are operating smoothly. Let me know if you need any additional details.\n\n"
                f"Best regards,\nAyyappa Rayudu"
            )

        # 4. Scenario C: Meeting / Reschedule / Delay
        is_meeting = bool(re.search(r'\b(meeting|late|reschedule|call|sync|delayed|traffic)\b', raw_lower))
        if is_meeting:
            clean_info = re.sub(r'^(?:late|delayed|about|saying|that)\s*', '', raw, flags=re.IGNORECASE).strip()
            return (
                f"Hello {recipient_display},\n\n"
                f"Regarding our scheduled meeting / discussion: {clean_info}.\n"
                f"Apologies for any inconvenience caused. Please let me know if we should adjust the timing.\n\n"
                f"Thanks,\nAyyappa"
            )

        # 5. Scenario D: Congratulations / Wishes / Celebration
        is_wishes = bool(re.search(r'\b(congrats|congratulations|happy\s*birthday|anniversary|celebrate|promotion|good\s*news)\b', raw_lower))
        if is_wishes:
            return f"Dear {recipient_display},\n\nHeartiest Congratulations! Wishing you continued success and the very best on this fantastic achievement.\n\nWarm regards,\nAyyappa Rayudu"

        # 6. Scenario E: Apology / Note
        is_apology = bool(re.search(r'\b(apolog|sorry|mistake|regret)\b', raw_lower))
        if is_apology:
            return f"Dear {recipient_display},\n\nI sincerely apologize regarding {raw}. I am actively addressing this and ensuring everything is aligned.\n\nBest regards,\nAyyappa"

        # Universal cleaned conversational prompt
        cleaned_msg = re.sub(r'^(?:saying|that|with\s+message|with\s+text|tell\s+(?:him|her|them)\s+(?:that)?|send\s+message\s+(?:about)?)\s*', '', raw, flags=re.IGNORECASE).strip(' "\'')
        return cleaned_msg or raw

    @classmethod
    def send_whatsapp_message(
        cls,
        recipient: Optional[str] = None,
        message: Optional[str] = None,
        auto_send: bool = True
    ) -> dict[str, Any]:
        """Natively controls WhatsApp to navigate to recipient and dispatch the message."""
        recipient = (recipient or "").strip()
        message = (message or "").strip()

        # Step 1: Check if recipient is a saved contact in database
        phone_number = None
        contact_name = recipient

        if recipient:
            if re.search(r'\+?\d{7,15}', recipient):
                phone_number = re.sub(r'[^\d+]', '', recipient)
            else:
                saved = db.get_contact(recipient)
                if saved and saved.get("phone"):
                    phone_number = re.sub(r'[^\d+]', '', saved["phone"])
                    contact_name = saved["name"]

        # If clean phone number is available: use direct protocol launch
        clean_phone_digits = re.sub(r'\D', '', phone_number or "")
        encoded_text = urllib.parse.quote_plus(message)

        if clean_phone_digits and len(clean_phone_digits) >= 7:
            app_url = f"whatsapp://send?phone={clean_phone_digits}&text={encoded_text}"
            web_url = f"https://web.whatsapp.com/send?phone={clean_phone_digits}&text={encoded_text}"

            # 1. Open direct WhatsApp protocol
            if cls._is_windows():
                try:
                    os.startfile(app_url)
                except Exception:
                    from backend.app.os_control.apps import _open_url_robust
                    _open_url_robust(web_url)
            else:
                from backend.app.os_control.apps import _open_url_robust
                _open_url_robust(web_url)

            # Auto-press Enter if desktop GUI automation is active
            if auto_send and cls._is_windows():
                try:
                    time.sleep(1.2)
                    cls.focus_whatsapp_window()
                    import pyautogui
                    pyautogui.press('enter')
                except Exception:
                    pass

            db.log_audit("OS_WHATSAPP", f"Sent WhatsApp to {clean_phone_digits} (length: {len(message)})", "INDRA_Controller", "SUCCESS")
            return {
                "action": "open_whatsapp",
                "status": "dispatched",
                "recipient": contact_name or clean_phone_digits,
                "phone": clean_phone_digits,
                "message": message,
                "app_target": app_url,
                "target": web_url,
                "client_action": {"type": "open_url", "url": web_url}
            }

        # Step 2: If only contact name is known (e.g. 'Rahul', 'Manager', 'Mom'):
        # Launch/focus WhatsApp and perform native GUI search + paste + send
        cls.launch_whatsapp()
        time.sleep(1.0)
        cls.focus_whatsapp_window()

        gui_executed = False
        if cls._is_windows() and contact_name:
            try:
                import pyautogui
                import pyperclip

                time.sleep(0.4)
                # Press Ctrl+F to open WhatsApp Search
                pyautogui.hotkey('ctrl', 'f')
                time.sleep(0.3)

                # Paste contact name into search
                pyperclip.copy(contact_name)
                pyautogui.hotkey('ctrl', 'v')
                time.sleep(0.6)

                # Select the contact
                pyautogui.press('enter')
                time.sleep(0.4)

                # If message is present, copy and paste it into the chat bar
                if message:
                    pyperclip.copy(message)
                    pyautogui.hotkey('ctrl', 'v')
                    time.sleep(0.3)

                    if auto_send:
                        pyautogui.press('enter')

                gui_executed = True
            except Exception as e:
                logger.warning(f"Native GUI WhatsApp automation error: {e}")

        # Provide fallback targets
        web_url = f"https://web.whatsapp.com/send?text={encoded_text}" if message else "https://web.whatsapp.com"
        app_url = f"whatsapp://send?text={encoded_text}" if message else "whatsapp:"

        db.log_audit("OS_WHATSAPP", f"WhatsApp OS automation for contact '{contact_name}'", "INDRA_Controller", "SUCCESS" if gui_executed else "FALLBACK")
        return {
            "action": "open_whatsapp",
            "status": "success" if gui_executed else "opened",
            "recipient": contact_name,
            "message": message,
            "app_target": app_url,
            "target": web_url,
            "client_action": {"type": "open_url", "url": web_url}
        }

whatsapp_controller = WhatsAppController()
