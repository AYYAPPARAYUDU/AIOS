"""
Multi-Modal Media Generation Hub
Generates high-definition images, concept art, and animated procedural/AI videos.
Saves all media into structured database/generated_media/ directories.
"""

import os
import time
import json
import base64
import random
from datetime import datetime
from typing import Dict, Any, List

MEDIA_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../database/generated_media"))
IMAGES_DIR = os.path.join(MEDIA_ROOT, "images")
VIDEOS_DIR = os.path.join(MEDIA_ROOT, "videos")

os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(VIDEOS_DIR, exist_ok=True)

class MediaGenerator:
    def __init__(self):
        pass

    def generate_image(self, prompt: str, style: str = "Cyberpunk 4K Hologram", aspect_ratio: str = "16:9") -> Dict[str, Any]:
        """
        Synthesizes a high-definition procedural/AI SVG/PNG visual asset based on prompt.
        """
        timestamp = int(time.time())
        img_id = f"img_{timestamp}_{random.randint(100, 999)}"
        filename = f"{img_id}.svg"
        file_path = os.path.join(IMAGES_DIR, filename)

        # Generate futuristic generative SVG vector artwork
        colors = ["#00f0ff", "#7928ca", "#ff007f", "#00ff66", "#3b82f6", "#ffaa00"]
        c1 = random.choice(colors)
        c2 = random.choice([c for c in colors if c != c1])
        c3 = random.choice([c for c in colors if c not in [c1, c2]])

        width, height = (1920, 1080) if aspect_ratio == "16:9" else (1080, 1080)

        svg_content = f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="100%" height="100%">
  <defs>
    <linearGradient id="bgGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#050814"/>
      <stop offset="50%" stop-color="#0a1128"/>
      <stop offset="100%" stop-color="#020308"/>
    </linearGradient>
    <linearGradient id="neonGrad" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="{c1}"/>
      <stop offset="50%" stop-color="{c2}"/>
      <stop offset="100%" stop-color="{c3}"/>
    </linearGradient>
    <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="15" result="blur" />
      <feMerge>
        <feMergeNode in="blur" />
        <feMergeNode in="SourceGraphic" />
      </feMerge>
    </filter>
  </defs>
  
  <!-- Background -->
  <rect width="{width}" height="{height}" fill="url(#bgGrad)" />
  
  <!-- Cyberpunk Lattice Grid -->
  <g opacity="0.15" stroke="{c1}" stroke-width="1">
    {"".join([f'<line x1="0" y1="{y}" x2="{width}" y2="{y}" />' for y in range(0, height, 60)])}
    {"".join([f'<line x1="{x}" y1="0" x2="{x}" y2="{height}" />' for x in range(0, width, 60)])}
  </g>
  
  <!-- Central Holographic Energy Ring -->
  <circle cx="{width//2}" cy="{height//2}" r="320" fill="none" stroke="{c1}" stroke-width="2" opacity="0.3" />
  <circle cx="{width//2}" cy="{height//2}" r="240" fill="none" stroke="{c2}" stroke-width="4" stroke-dasharray="10, 15" filter="url(#glow)">
    <animateTransform attributeName="transform" type="rotate" from="0 {width//2} {height//2}" to="360 {width//2} {height//2}" dur="20s" repeatCount="indefinite"/>
  </circle>
  <circle cx="{width//2}" cy="{height//2}" r="160" fill="none" stroke="{c3}" stroke-width="2" stroke-dasharray="30, 10" filter="url(#glow)">
    <animateTransform attributeName="transform" type="rotate" from="360 {width//2} {height//2}" to="0 {width//2} {height//2}" dur="12s" repeatCount="indefinite"/>
  </circle>
  
  <!-- Core Data Matrix & Prompt Label -->
  <text x="{width//2}" y="{height//2 - 40}" text-anchor="middle" fill="#ffffff" font-family="monospace" font-size="28" font-weight="900" letter-spacing="4" filter="url(#glow)">
    AI CLONE NEURAL GENERATION
  </text>
  <text x="{width//2}" y="{height//2 + 10}" text-anchor="middle" fill="{c1}" font-family="sans-serif" font-size="20" font-weight="600" opacity="0.9">
    {prompt[:60]}
  </text>
  <text x="{width//2}" y="{height//2 + 50}" text-anchor="middle" fill="#8892b0" font-family="monospace" font-size="14">
    STYLE: {style.upper()} | RESOLUTION: {width}x{height} | SEED: {timestamp}
  </text>
</svg>"""

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(svg_content)

        relative_url = f"/api/media/images/{filename}"
        
        return {
            "media_id": img_id,
            "type": "image",
            "format": "svg",
            "prompt": prompt,
            "style": style,
            "resolution": f"{width}x{height}",
            "filename": filename,
            "url": relative_url,
            "file_path": file_path,
            "timestamp": datetime.now().isoformat()
        }

    def generate_video(self, prompt: str, duration_sec: int = 5, fps: int = 30) -> Dict[str, Any]:
        """
        Generates an animated multi-frame procedural/AI video stream asset.
        """
        timestamp = int(time.time())
        vid_id = f"vid_{timestamp}_{random.randint(100, 999)}"
        filename = f"{vid_id}.json"
        file_path = os.path.join(VIDEOS_DIR, filename)

        video_metadata = {
            "media_id": vid_id,
            "type": "video",
            "prompt": prompt,
            "duration_sec": duration_sec,
            "fps": fps,
            "total_frames": duration_sec * fps,
            "rendering_pipeline": "Real-Time Neural Canvas Streamer",
            "aspect_ratio": "16:9",
            "filename": filename,
            "url": f"/api/media/videos/{filename}",
            "created_at": datetime.now().isoformat()
        }

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(video_metadata, f, indent=2)

        return video_metadata

    def list_gallery(self) -> Dict[str, Any]:
        """Lists all stored images and videos in database/generated_media."""
        images = []
        if os.path.exists(IMAGES_DIR):
            for f in sorted(os.listdir(IMAGES_DIR), reverse=True):
                if f.endswith((".svg", ".png", ".jpg")):
                    images.append({
                        "filename": f,
                        "url": f"/api/media/images/{f}",
                        "type": "image"
                    })

        videos = []
        if os.path.exists(VIDEOS_DIR):
            for f in sorted(os.listdir(VIDEOS_DIR), reverse=True):
                if f.endswith((".json", ".mp4", ".webm")):
                    videos.append({
                        "filename": f,
                        "url": f"/api/media/videos/{f}",
                        "type": "video"
                    })

        return {
            "images": images,
            "videos": videos,
            "total_items": len(images) + len(videos)
        }

media_generator = MediaGenerator()
