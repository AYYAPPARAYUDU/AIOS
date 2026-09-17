import sys
import os
sys.path.insert(0, os.path.abspath("."))
import asyncio
from backend.app.core.agents import agent_orchestrator

async def run_tests():
    queries = [
        "open youtube and play lofi",
        "open whatsapp",
        "set volume to 80",
        "search web for python 3.14",
        "clean ram",
        "system status"
    ]
    for q in queries:
        print(f"\n--- Testing query: '{q}' ---")
        res = await agent_orchestrator.process_user_query(q)
        print("Status:", "OK" if res.get("response") else "FAIL")
        print("Tools executed:", [tc['tool'] for tc in res.get('tool_calls', [])])
        print("Response snippet:", res['response'][:120])

if __name__ == "__main__":
    asyncio.run(run_tests())
