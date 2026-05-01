"""Soundwave Mood API.

Serves mood classifications for Spotify tracks via REST endpoints.
"""

import os
from datetime import datetime, timezone

import pandas as pd
from fastapi import FastAPI, HTTPException

from soundwave.config.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(title="Soundwave Mood API", version="2.0.0")

FEAST_PARQUET = os.environ.get("FEAST_PARQUET", "/app/feast/data/track_features.parquet")
_mood_data: dict = {}
_mood_descriptions: dict = {}


@app.on_event("startup")
def _load_data():
    """Load track mood data and mood descriptions at startup."""
    global _mood_data, _mood_descriptions

    try:
        df = pd.read_parquet(FEAST_PARQUET)
        _mood_data = df.set_index("track_id").to_dict("index")
    except Exception as e:
        logger.warning("Could not load mood data: %s", e)

    try:
        from soundwave.config.mood_config import GENRE_SUB_MOODS

        for moods in GENRE_SUB_MOODS.values():
            for name, defn in moods.items():
                _mood_descriptions[name] = defn["description"]
    except Exception:
        pass


@app.get("/health")
def health():
    """Return service health status."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/mood/{track_id}")
def get_mood(track_id: str):
    """Return mood classification for a given track ID.

    Raises:
        HTTPException: 404 if track_id is not found.
    """
    if track_id not in _mood_data:
        raise HTTPException(status_code=404, detail=f"Track '{track_id}' not found")

    track = _mood_data[track_id]
    mood_label = track.get("mood_cluster", "Unknown")
    return {
        "track_id": track_id,
        "genre_family": track.get("genre_family", "Unknown"),
        "mood_label": mood_label,
        "mood_description": _mood_descriptions.get(mood_label, ""),
        "danceability": round(float(track["danceability"]), 4),
        "energy": round(float(track["energy"]), 4),
        "valence": round(float(track["valence"]), 4),
    }
