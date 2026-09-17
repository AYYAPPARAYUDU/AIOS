from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

try:
    from backend.app.core.code_sandbox import code_sandbox
except ImportError:
    from app.core.code_sandbox import code_sandbox

router = APIRouter(prefix="/api/code", tags=["Complex Code Sandbox"])

class CodeRunRequest(BaseModel):
    language: str
    code: str
    timeout: Optional[int] = 15

@router.post("/execute")
async def execute_code_snippet(req: CodeRunRequest):
    """Executes code in Python, JavaScript, Bash, PowerShell in sandbox."""
    if not req.code.strip():
        raise HTTPException(status_code=400, detail="Code cannot be empty.")
    return code_sandbox.execute_code(language=req.language, code=req.code, timeout=req.timeout or 15)
