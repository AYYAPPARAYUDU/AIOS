import re

def extract_whatsapp_intent(clean: str):
    clean = re.sub(r'^(?:please|can you|could you|would you|jarvis|abhi|hey|help me|i want you to|just|go ahead and)\s+', '', clean, flags=re.IGNORECASE).strip()
    is_wa = bool(re.search(r'\b(whatsapp|whats\s*app|what\'s\s*app|message|msg|text)\b', clean, re.IGNORECASE))
    if not is_wa:
        return None

    phone_or_name = None
    message = None
    stopwords = {'whatsapp', 'whats', 'app', 'message', 'msg', 'text', 'the', 'a', 'in', 'on', 'via', 'to', 'for', 'saying', 'that'}

    def find_recipient(s: str):
        # 1. Match phone numbers (+919876543210, 9876543210, +1-555-123-4567, etc.)
        p_match = re.search(r'\+?\d[\d\-\s()]{6,16}\d', s)
        if p_match:
            digits = re.sub(r'\D', '', p_match.group(0))
            if len(digits) >= 7:
                return p_match.group(0).strip()
        # 2. Match named recipient after one or more prefix keywords (e.g. "message to rahul", "tell john", "to sarah")
        for n_match in re.finditer(r'(?:\b(?:to|for|message|msg|text|tell|whatsapp|send)\s+)+([a-zA-Z0-9_]+)', s, re.IGNORECASE):
            cand = n_match.group(1).strip()
            if cand.lower() not in stopwords:
                return cand
        return None

    # Colon syntax: e.g. "send message to 9876543210: Hello how are you"
    if ':' in clean:
        parts = clean.split(':', 1)
        prefix = parts[0].strip()
        message = parts[1].strip()
        phone_or_name = find_recipient(prefix)
    else:
        # Keyword syntax: saying <msg>, that <msg>, with message <msg>
        kw_match = re.search(r'\b(?:saying|that|with\s+message|with\s+text)\s+(.+)$', clean, re.IGNORECASE)
        if kw_match:
            message = kw_match.group(1).strip()
            prefix = clean[:kw_match.start()].strip()
            phone_or_name = find_recipient(prefix)
        else:
            phone_or_name = find_recipient(clean)
            if phone_or_name:
                idx = clean.find(phone_or_name)
                if idx != -1:
                    after = clean[idx + len(phone_or_name):].strip()
                    after = re.sub(r'^(?:saying|that|in\s+whatsapp|on\s+whatsapp|via\s+whatsapp|:\s*|,\s*)+', '', after, flags=re.IGNORECASE).strip()
                    after = re.sub(r'(?:in|on|via)?\s*whatsapp$', '', after, flags=re.IGNORECASE).strip()
                    if after and after.lower() not in stopwords:
                        message = after

    if message:
        message = re.sub(r'(?:in|on|via)?\s*whatsapp$', '', message, flags=re.IGNORECASE).strip(' "\'')

    params = {}
    if phone_or_name:
        params['phone'] = phone_or_name
    if message:
        params['message'] = message
    return {'tool': 'open_whatsapp', 'params': params}

queries = [
    'send a whatsapp message to 9876543210 saying hello how are you',
    'send message in whatsapp to 9999999999 saying hi',
    'send message to rahul saying are you free in whatsapp',
    'send message to 9876543210: Hello how are you',
    'whatsapp 9876543210 hello bro',
    'message John on whatsapp that meeting is at 5pm',
    'send a whatsapp message to +919876543210 saying let us meet',
    'open whatsapp',
    'tell Sarah on whatsapp that I will be late',
    'whatsapp message to mom saying love you'
]

if __name__ == '__main__':
    for q in queries:
        print(f"QUERY: '{q}'\n  -> RESULT: {extract_whatsapp_intent(q)}\n")
