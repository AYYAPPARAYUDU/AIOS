from fastapi import APIRouter
from pydantic import BaseModel
from typing import Any
from backend.app.core.mcp_registry import mcp_registry
from backend.app.core.agents import agent_orchestrator

router = APIRouter(prefix="/api/mcp", tags=["Model Context Protocol (MCP)"])

class MCPCallRequest(BaseModel):
    name: str
    arguments: dict[str, Any]

@router.get("/tools")
async def list_mcp_tools():
    """Returns available tools adhering to MCP specification."""
    return {"tools": mcp_registry.list_tools()}

@router.post("/execute")
async def execute_mcp_tool(req: MCPCallRequest):
    """Executes a tool via MCP interface."""
    res = await agent_orchestrator.execute_tool(req.name, req.arguments)
    return {"tool": req.name, "result": res}
