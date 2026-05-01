"""Spotify API client for fetching new releases and audio features."""

import base64
import os
import time
from datetime import datetime, timezone

import requests


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
        for attempt in range(5):
            resp = requests.get(
                f"{self.API_BASE}/{endpoint}",
                headers={"Authorization": f"Bearer {self._token}"},
                params=params,
                timeout=30,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 2**attempt))
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError(f"Spotify API rate limited after 5 retries: {endpoint}")

    def fetch_new_releases(self, limit: int = 50, total: int = 200) -> list[dict]:
        """Fetch new album releases and extract track IDs."""
        albums = []
        offset = 0
        while offset < total:
            data = self._get("browse/new-releases", {"limit": min(limit, total - offset), "offset": offset})
            items = data.get("albums", {}).get("items", [])
            if not items:
                break
            albums.extend(items)
            offset += len(items)
        return albums

    def fetch_album_tracks(self, album_id: str) -> list[dict]:
        """Fetch all tracks from an album."""
        data = self._get(f"albums/{album_id}/tracks", {"limit": 50})
        return data.get("items", [])

    def fetch_audio_features(self, track_ids: list[str]) -> list[dict]:
        """Fetch audio features for up to 100 tracks at a time."""
        features = []
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i : i + 100]
            data = self._get("audio-features", {"ids": ",".join(batch)})
            features.extend([f for f in data.get("audio_features", []) if f])
        return features

    def fetch_tracks(self, track_ids: list[str]) -> list[dict]:
        """Fetch track metadata for up to 50 tracks at a time."""
        tracks = []
        for i in range(0, len(track_ids), 50):
            batch = track_ids[i : i + 50]
            data = self._get("tracks", {"ids": ",".join(batch)})
            tracks.extend(data.get("tracks", []))
        return tracks

    def ingest_new_releases(self) -> list[dict]:
        """Full ingestion: fetch new releases, their tracks, and audio features.

        Returns list of dicts ready for DataFrame conversion, matching the
        bronze schema (track_id, track_name, artists, album_name, popularity,
        duration_ms, audio features).
        """
        albums = self.fetch_new_releases()
        print(f"Fetched {len(albums)} new release albums")

        track_ids = []
        track_album_map = {}
        for album in albums:
            album_tracks = self.fetch_album_tracks(album["id"])
            for t in album_tracks:
                track_ids.append(t["id"])
                track_album_map[t["id"]] = album["name"]

        print(f"Fetched {len(track_ids)} track IDs from albums")
        if not track_ids:
            return []

        tracks = self.fetch_tracks(track_ids)
        features = self.fetch_audio_features(track_ids)
        feature_map = {f["id"]: f for f in features}

        now = datetime.now(timezone.utc)
        rows = []
        for t in tracks:
            if not t:
                continue
            tid = t["id"]
            af = feature_map.get(tid, {})
            rows.append(
                {
                    "track_id": tid,
                    "track_name": t.get("name", ""),
                    "artists": ";".join(a["name"] for a in t.get("artists", [])),
                    "album_name": track_album_map.get(tid, t.get("album", {}).get("name", "")),
                    "track_genre": "",
                    "popularity": t.get("popularity", 0),
                    "duration_ms": t.get("duration_ms", 0),
                    "explicit": t.get("explicit", False),
                    "danceability": af.get("danceability", None),
                    "energy": af.get("energy", None),
                    "key": af.get("key", None),
                    "loudness": af.get("loudness", None),
                    "mode": af.get("mode", None),
                    "speechiness": af.get("speechiness", None),
                    "acousticness": af.get("acousticness", None),
                    "instrumentalness": af.get("instrumentalness", None),
                    "liveness": af.get("liveness", None),
                    "valence": af.get("valence", None),
                    "tempo": af.get("tempo", None),
                    "time_signature": af.get("time_signature", None),
                    "ingestion_timestamp": now,
                    "source": "spotify",
                }
            )

        print(f"Prepared {len(rows)} tracks for bronze ingestion")
        return rows
