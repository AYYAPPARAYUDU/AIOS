from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
import os

try:
    from backend.app.core.media_generator import media_generator, IMAGES_DIR, VIDEOS_DIR
except ImportError:
    from app.core.media_generator import media_generator, IMAGES_DIR, VIDEOS_DIR

router = APIRouter(prefix="/api/media", tags=["Multi-Modal Media Generation"])

class ImageGenRequest(BaseModel):
    prompt: str
    style: Optional[str] = "Cyberpunk 4K Hologram"
    aspect_ratio: Optional[str] = "16:9"

class VideoGenRequest(BaseModel):
    prompt: str
    duration_sec: Optional[int] = 5
    fps: Optional[int] = 30

@router.post("/generate-image")
async def generate_image(req: ImageGenRequest):
    """Generates a high-definition image based on prompt."""
    if not req.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty.")
    return media_generator.generate_image(
        prompt=req.prompt,
        style=req.style or "Cyberpunk 4K Hologram",
        aspect_ratio=req.aspect_ratio or "16:9"
    )

@router.post("/generate-video")
async def generate_video(req: VideoGenRequest):
    """Generates an animated multi-frame procedural/AI video asset."""
    if not req.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt cannot be empty.")
    return media_generator.generate_video(
        prompt=req.prompt,
        duration_sec=req.duration_sec or 5,
        fps=req.fps or 30
    )

@router.get("/gallery")
async def get_gallery():
    """Retrieves all generated images and videos from database/generated_media."""
    return media_generator.list_gallery()

@router.get("/images/{filename}")
async def serve_image(filename: str):
    file_path = os.path.join(IMAGES_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Image not found.")
    media_type = "image/svg+xml" if filename.endswith(".svg") else "image/png"
    return FileResponse(file_path, media_type=media_type)

@router.get("/videos/{filename}")
async def serve_video(filename: str):
    file_path = os.path.join(VIDEOS_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Video file not found.")
    return FileResponse(file_path, media_type="application/json")
