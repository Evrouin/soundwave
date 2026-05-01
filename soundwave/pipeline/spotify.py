"""Spotify API client for fetching new releases.

Works within Spotify Development Mode restrictions:
- Search limited to 10 results per request
- Individual track/album/artist lookups only (no bulk endpoints)
- No audio-features endpoint access
"""

import base64
import os
import time
from datetime import datetime, timezone

import requests

from soundwave.config.logger import get_logger

logger = get_logger(__name__)


class SpotifyClient:
    """Handles Spotify Web API authentication and data fetching."""

    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_BASE = "https://api.spotify.com/v1"

    def __init__(self, client_id: str = None, client_secret: str = None):
        """Initialize with Spotify credentials from args or environment."""
        self.client_id = client_id or os.environ["SPOTIFY_CLIENT_ID"]
        self.client_secret = client_secret or os.environ["SPOTIFY_CLIENT_SECRET"]
        self._token = None
        self._token_expires = 0

    def _authenticate(self):
        """Obtain access token via Client Credentials flow."""
        if time.time() < self._token_expires:
            return
        auth = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        resp = requests.post(
            self.TOKEN_URL,
            headers={"Authorization": f"Basic {auth}"},
            data={"grant_type": "client_credentials"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires = time.time() + data["expires_in"] - 60

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated GET request with retry and backoff."""
        self._authenticate()
        for attempt in range(3):
            resp = requests.get(
                f"{self.API_BASE}/{endpoint}",
                headers={"Authorization": f"Bearer {self._token}"},
                params=params,
                timeout=10,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 3))
                logger.warning("Rate limited, waiting %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Spotify API rate limited after 3 retries: {endpoint}")

    def fetch_new_albums(self, total: int = 10) -> list[dict]:
        """Fetch new albums via search (10 per request, dev mode limit)."""
        data = self._get("search", {"q": "tag:new", "type": "album", "limit": 10, "offset": 0})
        return data.get("albums", {}).get("items", [])

    def fetch_album_tracks(self, album_id: str) -> list[dict]:
        """Fetch all tracks from an album."""
        data = self._get(f"albums/{album_id}/tracks", {"limit": 50})
        return data.get("items", [])

    def fetch_track(self, track_id: str) -> dict:
        """Fetch single track metadata (dev mode: individual lookup only)."""
        return self._get(f"tracks/{track_id}")

    def ingest_new_releases(self) -> list[dict]:
        """Fetch new release albums, extract tracks, and prepare for bronze.

        Works within dev mode limits: search (10/req), album tracks, single track lookup.
        Audio features are not available in dev mode and will be null.
        """
        albums = self.fetch_new_albums()
        logger.info("Fetched %d new release albums", len(albums))

        now = datetime.now(timezone.utc)
        rows = []
        for album in albums:
            try:
                album_tracks = self.fetch_album_tracks(album["id"])
            except Exception as e:
                logger.warning("Skipping album %s: %s", album.get("name"), e)
                continue
            time.sleep(1)
            for t in album_tracks:
                rows.append({
                    "track_id": t["id"],
                    "track_name": t.get("name", ""),
                    "artists": ";".join(a["name"] for a in t.get("artists", [])),
                    "album_name": album.get("name", ""),
                    "track_genre": "",
                    "popularity": 0,
                    "duration_ms": t.get("duration_ms", 0),
                    "explicit": t.get("explicit", False),
                    "danceability": None,
                    "energy": None,
                    "key": None,
                    "loudness": None,
                    "mode": None,
                    "speechiness": None,
                    "acousticness": None,
                    "instrumentalness": None,
                    "liveness": None,
                    "valence": None,
                    "tempo": None,
                    "time_signature": None,
                    "ingestion_timestamp": now,
                    "source": "spotify",
                })

        logger.info("Prepared %d tracks for bronze ingestion", len(rows))
        return rows
