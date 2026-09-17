import re
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from backend.app.os_control.whatsapp import whatsapp_controller

def parse_whatsapp_nlp(clean: str):
    clean_norm = re.sub(r'^(?:please|can you|could you|would you|jarvis|abhi|hey|help me|i want you to|just|go ahead and)\s+', '', clean, flags=re.IGNORECASE).strip()
    
    is_wa = bool(re.search(r'\b(whatsapp|whats\s*app|what\'s\s*app|message|msg|text|leave\s*letter|leave\s*application|leave\s*request|sick\s*note|leave\s*note)\b', clean_norm, re.IGNORECASE))
    if not is_wa:
        return None

    stopwords = {'whatsapp', 'whats', 'app', 'message', 'msg', 'text', 'the', 'a', 'in', 'on', 'via', 'to', 'for', 'saying', 'that', 'send', 'letter', 'leave', 'reason', 'about', 'and', 'my'}
    time_words = {'today', 'tomorrow', 'yesterday', 'morning', 'evening', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'days', 'day', 'week', 'month'}

    recipient = None
    reason = None
    message = None

    # 1. Look for Phone Number (+91..., 9876...)
    p_match = re.search(r'\+?\d[\d\-\s()]{6,16}\d', clean_norm)
    if p_match:
        digits = re.sub(r'\D', '', p_match.group(0))
        if len(digits) >= 7:
            recipient = p_match.group(0).strip()

    # 2. Extract Named Recipient
    if not recipient:
        # Pattern A: Search for contact [name]
        m_contact = re.search(r'(?:search\s+(?:for\s+)?(?:whatsapp\s+)?contact\s+|contact\s+)([a-zA-Z0-9_]+)', clean_norm, re.IGNORECASE)
        if m_contact and m_contact.group(1).lower() not in stopwords and m_contact.group(1).lower() not in time_words:
            recipient = m_contact.group(1).strip()

        # Pattern B: to (my) (manager/boss) [name]
        if not recipient:
            m_mgr = re.search(r'\bto\s+(?:my\s+)?(manager|boss|colleague|team\s*lead|hr|lead|teacher|principal|prof|sir|madam)(?:\s+([a-zA-Z0-9_]+))?', clean_norm, re.IGNORECASE)
            if m_mgr:
                title = m_mgr.group(1).strip()
                name = m_mgr.group(2).strip() if m_mgr.group(2) else None
                if name and name.lower() not in stopwords and name.lower() not in time_words:
                    recipient = name
                else:
                    recipient = title.capitalize()

        # Pattern C: to/for/tell/whatsapp [name]
        if not recipient:
            for n_match in re.finditer(r'(?:\b(?:to|for|tell|whatsapp|message|msg|text)\s+)+([a-zA-Z0-9_]+)', clean_norm, re.IGNORECASE):
                cand = n_match.group(1).strip()
                if cand.lower() not in stopwords and cand.lower() not in time_words:
                    recipient = cand
                    break

    # 3. Check if Leave Letter intent
    is_leave = bool(re.search(r'\b(leave\s*letter|leave\s*note|leave\s*application|leave\s*request|sick\s*leave|vacation\s*leave|apply\s*leave)\b', clean_norm, re.IGNORECASE))
    if not is_leave and ("leave" in clean_norm.lower() and ("reason" in clean_norm.lower() or "fever" in clean_norm.lower() or "sick" in clean_norm.lower() or "headache" in clean_norm.lower())):
        is_leave = True

    if is_leave:
        # Extract reason
        m_reason = re.search(r'\b(?:on|due\s+to|because\s+of|with\s+reason|reason\s*[:=]?)\s+([a-zA-Z0-9_\s]+?)(?:\s+(?:to|on\s+whatsapp|in\s+whatsapp|via\s+whatsapp|for\s+tomorrow|for\s+today)|$)', clean_norm, re.IGNORECASE)
        if m_reason:
            cand_reason = m_reason.group(1).strip()
            # remove words like "fever reason" -> "fever"
            cand_reason = re.sub(r'\s+reason$', '', cand_reason, flags=re.IGNORECASE).strip()
            if cand_reason and cand_reason.lower() not in ["this", "this reason", "reason"]:
                reason = cand_reason

        message = whatsapp_controller.generate_smart_context(
            prompt_or_reason=reason or clean_norm,
            recipient=recipient or "Sir/Madam",
            context_type="leave_letter"
        )
    else:
        # Regular message extraction
        # Colon syntax
        if ':' in clean_norm:
            parts = clean_norm.split(':', 1)
            message = parts[1].strip()
        else:
            # saying / that / with message / about
            kw_match = re.search(r'\b(?:saying|that|with\s+message|with\s+text|about)\s+(.+)$', clean_norm, re.IGNORECASE)
            if kw_match:
                message = kw_match.group(1).strip()
            elif recipient:
                # find text after recipient
                idx = clean_norm.find(recipient)
                if idx != -1:
                    after = clean_norm[idx + len(recipient):].strip()
                    after = re.sub(r'^(?:saying|that|in\s+whatsapp|on\s+whatsapp|via\s+whatsapp|:\s*|,\s*|and\s+send\s+message\s+about\s*|and\s+send\s+message\s*)+', '', after, flags=re.IGNORECASE).strip()
                    after = re.sub(r'(?:in|on|via)?\s*whatsapp$', '', after, flags=re.IGNORECASE).strip()
                    if after and after.lower() not in stopwords:
                        message = after

        if message:
            message = re.sub(r'(?:in|on|via)?\s*whatsapp$', '', message, flags=re.IGNORECASE).strip(' "\'')
            message = whatsapp_controller.generate_smart_context(message, recipient=recipient)

    params = {}
    if recipient:
        params["phone"] = recipient
    if message:
        params["message"] = message

    return {"tool": "open_whatsapp", "params": params}

if __name__ == "__main__":
    tests = [
        "open whatsapp",
        "send leave letter on this reason",
        "send leave letter on fever reason to my manager rahul on whatsapp",
        "open whats imediatly and search for whatsapp contact rahul and send message about project update",
        "send message to 9876543210: Hello how are you",
        "whatsapp +919876543210 saying let us meet",
        "send leave letter for tomorrow due to severe headache to boss",
        "tell Sarah on whatsapp that I will be late"
    ]
    for t in tests:
        res = parse_whatsapp_nlp(t)
        print(f"\nQUERY: {t}\n-> RECIPIENT: {res['params'].get('phone')}\n-> MESSAGE:\n{res['params'].get('message')}\n")
