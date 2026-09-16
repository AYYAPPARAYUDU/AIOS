import time
import re
from typing import Any

class EmotionEngine:
    """Analyzes user emotion, stress indicators, focus level, and cognitive state in real-time."""

    def __init__(self):
        self.current_emotion = {
            "mood": "focused",
            "stress_level": 15,
            "focus_score": 88,
            "sentiment": "positive",
            "heart_rate_sim": 72,
            "energy_level": 85,
            "last_updated": time.time(),
            "recommendation": "Optimal focus detected. Deep work protocols engaged."
        }
        self.history: list[dict[str, Any]] = []

    def analyze_input(self, text: str) -> dict[str, Any]:
        """Evaluates user text sentiment, cognitive pressure, and emotional markers."""
        t = text.lower().strip()
        
        stress_score = 15
        focus_score = 80
        energy_score = 80
        mood = "focused"
        sentiment = "neutral"
        recommendation = "Cognitive baseline nominal."

        # Stress indicators
        stress_words = ["stressed", "tired", "headache", "urgent", "broken", "bug", "crash", "angry", "frustrated", "late", "hate", "slow", "annoyed", "exhausted"]
        stress_hits = sum(1 for w in stress_words if w in t)
        
        # Positive indicators
        happy_words = ["great", "awesome", "perfect", "good", "love", "thanks", "thank you", "nice", "cool", "super", "happy", "fast"]
        happy_hits = sum(1 for w in happy_words if w in t)

        # Tired / Fatigue indicators
        tired_words = ["sleepy", "tired", "night", "exhausted", "done for today", "drowsy", "head hurting"]
        tired_hits = sum(1 for w in tired_words if w in t)

        if stress_hits > 0:
            stress_score = min(95, 40 + (stress_hits * 20))
            mood = "stressed"
            sentiment = "negative"
            focus_score = max(30, 80 - (stress_hits * 15))
            recommendation = "Elevated stress detected. Would you like me to enable Focus Protocol and reduce display glare?"

        elif tired_hits > 0:
            mood = "tired"
            energy_score = 30
            focus_score = 45
            sentiment = "neutral"
            recommendation = "Fatigue detected, sir. Consider taking a 5-minute break or enabling Night Protocol."

        elif happy_hits > 0:
            mood = "happy"
            sentiment = "positive"
            stress_score = 10
            energy_score = 90
            focus_score = 92
            recommendation = "Positive valence detected. Systems operating at peak cognitive synergy."

        elif len(text.split()) > 15:
            mood = "focused"
            focus_score = 95
            stress_score = 20
            sentiment = "positive"
            recommendation = "Deep analytical state active."

        self.current_emotion = {
            "mood": mood,
            "stress_level": stress_score,
            "focus_score": focus_score,
            "sentiment": sentiment,
            "heart_rate_sim": 70 + int(stress_score * 0.25),
            "energy_level": energy_score,
            "last_updated": time.time(),
            "recommendation": recommendation
        }

        self.history.append(self.current_emotion)
        if len(self.history) > 50:
            self.history.pop(0)

        return self.current_emotion

    def get_current_emotion(self) -> dict[str, Any]:
        return self.current_emotion

emotion_engine = EmotionEngine()
