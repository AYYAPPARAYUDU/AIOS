import os
import sys

# Ensure backend root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.app.clone.evolution_analytics import evolution_analytics
from backend.app.clone.perception_engine import perception_engine
from backend.app.clone.continuous_learner import continuous_learner
from backend.app.core.code_sandbox import code_sandbox
from backend.app.core.media_generator import media_generator
from backend.app.storage.capacity_monitor import capacity_monitor

def test_evolution_analytics():
    metrics = evolution_analytics.get_evolution_metrics()
    assert metrics["status"] == "success"
    assert len(metrics["sectors"]) == 6
    assert len(metrics["radar_distribution"]) == 6
    assert len(metrics["daily_history"]) > 0
    assert metrics["current_snapshot"]["learning_velocity_pct"] > 0

def test_perception_engine():
    res = perception_engine.process_telemetry(
        visual_data={"brightness": 0.8, "motion_intensity": 0.2, "face_detected": True},
        audio_data={"volume": 0.6, "pitch": 0.5}
    )
    assert "dominant_emotion" in res
    assert "clone_mirror_response" in res
    assert res["attention_level"] > 0

def test_code_sandbox_python():
    res = code_sandbox.execute_code(
        language="python",
        code="print('JARVIS_NEURAL_EXEC_SUCCESS', 42 * 2)"
    )
    assert res["success"] is True
    assert "JARVIS_NEURAL_EXEC_SUCCESS 84" in res["stdout"]

def test_media_generator_image():
    res = media_generator.generate_image(prompt="Cybernetic neural brain core 4K", style="Cyberpunk")
    assert res["type"] == "image"
    assert os.path.exists(res["file_path"])

def test_capacity_monitor():
    cap = capacity_monitor.check_capacity()
    assert cap["allocated_gb"] >= 200.0
    assert "breakdown" in cap
    assert len(cap["breakdown"]) >= 5

if __name__ == "__main__":
    test_evolution_analytics()
    test_perception_engine()
    test_code_sandbox_python()
    test_media_generator_image()
    test_capacity_monitor()
    print("ALL EVOLUTION, PERCEPTION, SANDBOX & MEDIA TESTS PASSED SUCCESSFULLY!")
