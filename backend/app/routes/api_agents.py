from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Any
from backend.app.core.agents import agent_orchestrator
from backend.app.core.llm import ollama_client
from backend.app.storage.db import db

router = APIRouter(prefix="/api/agents", tags=["Multi-Agent System & LLM"])

class ChatRequest(BaseModel):
    message: str
    target_agent: Optional[str] = "abhi"
    conversation_id: Optional[str] = "main_session"
    model: Optional[str] = None

class DispatchRequest(BaseModel):
    agent_id: str
    instruction: str
    conversation_id: Optional[str] = "main_session"

class ToolExecuteRequest(BaseModel):
    tool: str
    parameters: dict[str, Any]

class ContactRequest(BaseModel):
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    notes: Optional[str] = None

class SessionActionRequest(BaseModel):
    conversation_id: Optional[str] = "main_session"

@router.get("/status")
async def get_agents_status():
    return {
        "agents": agent_orchestrator.get_agents_status(),
        "recent_logs": agent_orchestrator.agent_logs[-15:],
        "llm_health": await ollama_client.check_health()
    }

@router.post("/chat")
async def chat_with_jarvis(req: ChatRequest):
    result = await agent_orchestrator.process_user_query(
        query=req.message,
        target_agent=req.target_agent or "abhi",
        selected_agent=req.target_agent or "abhi",
        conversation_id=req.conversation_id or "main_session"
    )
    return result

@router.post("/dispatch")
async def dispatch_agent(req: DispatchRequest):
    result = await agent_orchestrator.process_user_query(
        query=req.instruction,
        target_agent=req.agent_id or "abhi",
        selected_agent=req.agent_id or "abhi",
        conversation_id=req.conversation_id or "main_session"
    )
    return result

@router.post("/tool-call")
async def run_direct_tool(req: ToolExecuteRequest):
    res = await agent_orchestrator.execute_tool(req.tool, req.parameters)
    return {"tool": req.tool, "result": res}

@router.get("/messages")
async def get_conversation_history(conversation_id: str = "main_session", limit: int = 50):
    return {
        "conversation_id": conversation_id,
        "messages": db.get_messages(conversation_id, limit=limit)
    }

@router.get("/logs")
async def get_agent_logs():
    return {"logs": agent_orchestrator.agent_logs[-30:]}

@router.get("/memories")
async def list_memories(category: Optional[str] = None):
    return {"memories": db.list_memories(category=category)}

# Contacts Directory Endpoints (WhatsApp, Email & Phonebook Memory)
@router.get("/contacts")
async def list_contacts():
    return {"contacts": db.list_contacts()}

@router.post("/contacts")
async def save_contact(req: ContactRequest):
    res = db.save_contact(name=req.name, phone=req.phone, email=req.email, notes=req.notes)
    return res

@router.delete("/contacts/{name}")
async def delete_contact(name: str):
    success = db.delete_contact(name)
    return {"deleted": success, "contact": name}

# Session Management (Export to Knowledge Vault vs Clear)
@router.post("/session/export")
async def export_session(req: SessionActionRequest):
    res = db.export_session_to_vault(req.conversation_id or "main_session")
    return res

@router.delete("/session/clear")
async def clear_session(conversation_id: str = "main_session"):
    success = db.clear_conversation(conversation_id)
    return {"status": "cleared", "conversation_id": conversation_id, "success": success}

# Continuous ML/DL Self-Training & Dynamic Decision Tree Telemetry
@router.get("/evolution/trees")
async def get_evolution_decision_trees():
    from backend.app.clone.ml_dl_trainer import ml_dl_trainer
    return {
        "status": "success",
        "tree": ml_dl_trainer.get_decision_tree_graph(),
        "dataset": ml_dl_trainer.get_dataset_stats()
    }

@router.get("/evolution/pipeline")
async def get_training_pipeline():
    from backend.app.clone.ml_dl_trainer import ml_dl_trainer
    return {
        "status": "success",
        "stages": ml_dl_trainer.get_training_pipeline_stages(),
        "stats": ml_dl_trainer.get_dataset_stats()
    }

@router.post("/train")
async def trigger_active_training():
    from backend.app.clone.ml_dl_trainer import ml_dl_trainer
    res = ml_dl_trainer.train_step(batch_size=32)
    return res
