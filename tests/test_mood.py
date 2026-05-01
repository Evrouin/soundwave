"""Tests for mood classification logic and configuration integrity."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from soundwave.config.mood_config import GENRE_FAMILY_MAP, GENRE_SUB_MOODS, UNIVERSAL_MOODS, classify_universal_mood


def test_all_genres_mapped():
    """Every genre should map to a valid family in GENRE_SUB_MOODS."""
    for genre, family in GENRE_FAMILY_MAP.items():
        assert family in GENRE_SUB_MOODS, f"{genre} → {family} not in GENRE_SUB_MOODS"


def test_each_family_has_5_sub_moods():
    """Each genre family must have exactly 5 sub-mood labels."""
    for family, moods in GENRE_SUB_MOODS.items():
        assert len(moods) == 5, f"{family} has {len(moods)} sub-moods, expected 5"


def test_universal_moods_has_10():
    """There must be exactly 10 universal mood labels."""
    assert len(UNIVERSAL_MOODS) == 10


def test_comedy_gen_hoshino_is_relaxed():
    """Comedy by Gen Hoshino: low energy, high valence → Relaxed."""
    mood = classify_universal_mood(
        energy=0.461,
        valence=0.715,
        danceability=0.676,
        acousticness=0.0322,
        instrumentalness=0.0438,
        tempo_norm=0.25,
        loudness_norm=0.89,
        speechiness=0.143,
        liveness=0.139,
        mode=0,
        genre_family="Pop",
    )
    assert mood == "Relaxed"


def test_high_energy_low_valence_is_aggressive():
    """High energy + low valence → Aggressive."""
    mood = classify_universal_mood(
        energy=0.95,
        valence=0.15,
        danceability=0.4,
        acousticness=0.01,
        instrumentalness=0.0,
        tempo_norm=0.8,
        loudness_norm=0.95,
        speechiness=0.05,
        liveness=0.1,
        mode=0,
        genre_family="Metal/Hardcore",
    )
    assert mood == "Aggressive"


def test_high_energy_high_valence_high_dance_is_euphoric():
    """High everything positive → Euphoric."""
    mood = classify_universal_mood(
        energy=0.85,
        valence=0.9,
        danceability=0.85,
        acousticness=0.05,
        instrumentalness=0.0,
        tempo_norm=0.7,
        loudness_norm=0.85,
        speechiness=0.05,
        liveness=0.3,
        mode=1,
        genre_family="Pop",
    )
    assert mood == "Euphoric"


def test_low_valence_low_energy_is_melancholic():
    """Low valence + low energy → Melancholic."""
    mood = classify_universal_mood(
        energy=0.2,
        valence=0.1,
        danceability=0.3,
        acousticness=0.8,
        instrumentalness=0.1,
        tempo_norm=0.2,
        loudness_norm=0.3,
        speechiness=0.03,
        liveness=0.1,
        mode=0,
        genre_family="Pop",
    )
    assert mood == "Melancholic"


def test_genre_bias_jazz_pushes_groovy():
    """Borderline track in Jazz/Soul should lean Groovy."""
    mood = classify_universal_mood(
        energy=0.55,
        valence=0.55,
        danceability=0.72,
        acousticness=0.1,
        instrumentalness=0.1,
        tempo_norm=0.4,
        loudness_norm=0.6,
        speechiness=0.05,
        liveness=0.2,
        mode=1,
        genre_family="Jazz/Soul",
    )
    assert mood == "Groovy"
