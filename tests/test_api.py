"""Tests for the FastAPI mood API."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from fastapi.testclient import TestClient


def test_health():
    from main import app

    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "timestamp" in data


def test_mood_not_found():
    from main import app

    client = TestClient(app)
    r = client.get("/mood/nonexistent_track_id")
    assert r.status_code == 404
    assert "not found" in r.json()["detail"].lower()
