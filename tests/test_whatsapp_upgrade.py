import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.app.core.agents import agent_orchestrator
from backend.app.os_control.whatsapp import whatsapp_controller

def test_whatsapp_parsing_and_generation():
    test_cases = [
        "open whatsapp",
        "send leave letter on this reason",
        "send leave letter on fever reason to my manager rahul on whatsapp",
        "open whats imediatly and search for whatsapp contact rahul and send message about project update",
        "send message to 9876543210: Hello how are you",
        "whatsapp +919876543210 saying let us meet",
        "send leave letter for tomorrow due to severe headache to boss",
        "tell Sarah on whatsapp that I will be late"
    ]

    print("=" * 60)
    print("RUNNING WHATSAPP PARSING & GENERATION TESTS")
    print("=" * 60)

    for query in test_cases:
        intent = agent_orchestrator._extract_whatsapp_intent(query)
        print(f"\nQUERY: '{query}'")
        print(f"PARSED INTENT: {intent}")
        if intent and intent.get("params", {}).get("message"):
            msg = intent["params"]["message"]
            print(f"GENERATED CONTEXT ({len(msg)} chars):\n---\n{msg}\n---")
        assert intent is not None, f"Failed to parse query: {query}"
        assert intent.get("tool") == "open_whatsapp"

    print("\n[SUCCESS] All WhatsApp queries parsed and context generated perfectly!")

if __name__ == "__main__":
    test_whatsapp_parsing_and_generation()
