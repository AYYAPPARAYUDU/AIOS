from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query
from pydantic import BaseModel
from typing import Optional, Any

try:
    from backend.app.storage.manager import storage_mgr
    from backend.app.storage.capacity_monitor import capacity_monitor
except ImportError:
    from app.storage.manager import storage_mgr
    from app.storage.capacity_monitor import capacity_monitor

router = APIRouter(prefix="/api/storage", tags=["Dynamic 200GB Storage Vault & Sentinel"])

class IndexDirectoryRequest(BaseModel):
    directory_path: str
    max_files: Optional[int] = 300

class SearchRequest(BaseModel):
    query: str

class ExpandStorageRequest(BaseModel):
    additional_gb: Optional[float] = 100.0

@router.get("/stats")
async def get_storage_stats():
    return storage_mgr.get_pool_stats()

@router.get("/capacity")
async def get_capacity_stats():
    """Returns dynamic 200GB capacity breakdown and checks if limits are near to alert the user."""
    return capacity_monitor.check_capacity()

@router.post("/expand")
async def expand_vault_storage(req: ExpandStorageRequest):
    """Expands storage allocation dynamically."""
    return capacity_monitor.expand_storage(req.additional_gb or 100.0)

@router.get("/files")
async def list_files(subfolder: str = Query(default="")):
    return {"files": storage_mgr.list_pool_files(subfolder=subfolder)}

@router.post("/upload")
async def upload_file_to_pool(file: UploadFile = File(...), subfolder: str = Form(default="documents")):
    try:
        content = await file.read()
        res = storage_mgr.save_file_to_pool(
            filename=file.filename,
            content=content,
            subfolder=subfolder
        )
        return res
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/files")
async def delete_file(file_path: str = Query(...)):
    try:
        success = storage_mgr.delete_pool_file(file_path)
        return {"status": "success" if success else "failed", "deleted": file_path}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/index-directory")
async def index_laptop_directory(req: IndexDirectoryRequest):
    count = storage_mgr.scan_and_index_directory(req.directory_path, req.max_files or 300)
    return {"status": "success", "indexed_count": count, "directory": req.directory_path}

@router.post("/search")
async def search_storage_knowledge(req: SearchRequest):
    results = storage_mgr.semantic_or_keyword_search(req.query)
    return {"query": req.query, "count": len(results), "results": results}
