"""FastAPI mood classification API (FR-SRV-01 through FR-SRV-04)."""
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Soundwave Mood API", version="2.0.0")

FEAST_PARQUET = os.environ.get("FEAST_PARQUET", "/app/feast/data/track_features.parquet")
_mood_data: dict = {}
_mood_descriptions: dict = {}


@app.on_event("startup")
def load_data():
    """Load track mood data and mood descriptions."""
    global _mood_data, _mood_descriptions

    try:
        df = pd.read_parquet(FEAST_PARQUET)
        _mood_data = df.set_index("track_id").to_dict("index")
        print(f"Loaded {len(_mood_data)} tracks")
    except Exception as e:
        print(f"Warning: Could not load mood data: {e}")

    # Load mood descriptions from config
    try:
        sys.path.insert(0, "/app/dags")
        from mood_config import GENRE_SUB_MOODS
        for family, moods in GENRE_SUB_MOODS.items():
            for mood_name, mood_def in moods.items():
                _mood_descriptions[mood_name] = mood_def["description"]
    except Exception as e:
        print(f"Warning: Could not load mood descriptions: {e}")


@app.get("/health")
def health():
    """Health check endpoint (FR-SRV-04)."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/mood/{track_id}")
def get_mood(track_id: str):
    """Return mood classification for a track (FR-SRV-01, FR-SRV-02)."""
    if track_id not in _mood_data:
        raise HTTPException(status_code=404, detail=f"Track '{track_id}' not found")
    t = _mood_data[track_id]
    mood = t["mood_cluster"]
    return {
        "track_id": track_id,
        "genre_family": t.get("genre_family", "Unknown"),
        "mood_label": mood,
        "mood_description": _mood_descriptions.get(mood, ""),
        "danceability": round(float(t["danceability"]), 4),
        "energy": round(float(t["energy"]), 4),
        "valence": round(float(t["valence"]), 4),
    }
