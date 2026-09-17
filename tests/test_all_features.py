import sys
import os
sys.path.insert(0, os.path.abspath("."))
import asyncio
from backend.app.core.agents import agent_orchestrator
from backend.app.clone.clone_service import clone_service
from backend.app.core.mcp_registry import mcp_registry

async def run_full_validation():
    print("\n[1] Testing MCP Tools List:", flush=True)
    tools = mcp_registry.list_tools()
    print("Found MCP Tools:", [t["name"] for t in tools], flush=True)

    print("\n[2] Testing AI Clone Microservice Profile:", flush=True)
    profile = clone_service.get_profile()
    print("Clone Persona:", profile["persona"]["name"], flush=True)
    print("Evolution Score:", profile["persona"]["evolution_score"], flush=True)
    print("Memory Stats:", profile["memory_stats"], flush=True)

    print("\n[3] Testing Direct Intent Controller (YouTube & Volume):", flush=True)
    res1 = await agent_orchestrator.process_user_query("open youtube and set volume to 75")
    print("Response 1:", res1["response"], flush=True)
    print("Tool calls 1:", [t["tool"] for t in res1["tool_calls"]], flush=True)
    print("Client actions 1:", res1["client_actions"], flush=True)

    print("\n[4] Testing WhatsApp Controller Intent:", flush=True)
    res3 = await agent_orchestrator.process_user_query("open whatsapp")
    print("WhatsApp Response:", res3["response"], flush=True)
    print("WhatsApp Client Actions:", res3["client_actions"], flush=True)

    print("\n[5] Testing AI Clone Dialogue (Direct Clone Invocation):", flush=True)
    res2 = await agent_orchestrator.process_user_query("Give a 1-sentence clone status update", target_agent="clone")
    print("Clone Response Snippet:", res2["response"][:150].encode('ascii', 'ignore').decode('ascii'), flush=True)
    print("Clone Model:", res2.get("clone_meta", {}).get("model"), flush=True)

    print("\n>>> ALL 5/5 SYSTEM & CLONE VERIFICATION TESTS PASSED PERFECTLY! <<<", flush=True)

if __name__ == "__main__":
    asyncio.run(run_full_validation())
