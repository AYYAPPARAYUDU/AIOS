import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.app.clone.ml_dl_trainer import ml_dl_trainer
from backend.app.os_control.whatsapp import whatsapp_controller
from backend.app.core.agents import agent_orchestrator

def test_ml_dl_and_scenarios():
    print("=" * 60)
    print("TESTING COMPACT ML/DL DATASET INGESTION & TRAINING")
    print("=" * 60)

    # Ingest diverse interactions
    samples = [
        ("open whatsapp and send leave letter on fever reason to rahul", "Leave letter context generated and WhatsApp opened", [{"tool": "open_whatsapp"}], "indra"),
        ("set master volume to 80", "Set master volume to 80%", [{"tool": "set_volume"}], "indra"),
        ("search web for quantum computing trends", "Narada retrieved 10 sources", [{"tool": "search_web"}], "narada"),
        ("write an apology note for being late to meeting and send to manager", "Apology drafted and dispatched", [{"tool": "open_whatsapp"}], "indra"),
        ("Clone Mode: analyze my architecture sprint", "Analyzed sprint architecture", None, "clone")
    ]

    for q, r, tc, agent in samples:
        ml_dl_trainer.ingest_interaction(q, r, tc, agent)

    stats = ml_dl_trainer.get_dataset_stats()
    print(f"Dataset stats: {stats}")
    assert stats["total_samples"] >= 5

    # Run active training step
    train_res = ml_dl_trainer.train_step(batch_size=16)
    print(f"\nTraining step result: {train_res}")
    assert train_res["status"] == "success"
    assert "loss" in train_res
    assert "accuracy" in train_res

    # Test Decision Tree graph serialization
    tree_graph = ml_dl_trainer.get_decision_tree_graph()
    print(f"\nDecision Tree Algorithm: {tree_graph['algorithm']}")
    print(f"Root node feature: {tree_graph['root']['feature']}")
    assert "root" in tree_graph

    # Test 6-Stage Pipeline
    stages = ml_dl_trainer.get_training_pipeline_stages()
    print(f"\nPipeline Stages count: {len(stages)}")
    assert len(stages) == 6

    print("\n" + "=" * 60)
    print("TESTING UNIVERSAL SCENARIO GENERATOR (OLLAMA + TEMPLATES)")
    print("=" * 60)

    scenarios = [
        ("send leave letter for tomorrow due to high fever", "Rahul"),
        ("send project update that backend database migration is 100% complete", "Priya"),
        ("tell my friend John on whatsapp congratulations on your new job promotion", "John"),
        ("write apology note for being late to meeting because of heavy traffic and send to manager", "Manager"),
        ("send dinner party invitation for Saturday 8pm at my place", "Alex")
    ]

    for prompt, recipient in scenarios:
        generated = whatsapp_controller.generate_smart_context(prompt_or_reason=prompt, recipient=recipient)
        print(f"\n--- SCENARIO: '{prompt}' (Recipient: {recipient}) ---")
        print(generated)
        assert generated is not None and len(generated) > 10

    print("\n>>> ALL ML/DL AND MULTI-SCENARIO TESTS PASSED PERFECTLY! <<<")

if __name__ == "__main__":
    test_ml_dl_and_scenarios()
