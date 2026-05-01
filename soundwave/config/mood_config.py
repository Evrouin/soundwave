"""Mood classification configuration: genre mappings, sub-mood definitions, and universal mood rules."""

UNIVERSAL_MOODS = {
    "Aggressive": {"description": "Intense, angry, and raw"},
    "Euphoric": {"description": "Joyful, uplifting, peak happiness"},
    "Energetic": {"description": "High-energy, pumped, and powerful"},
    "Groovy": {"description": "Rhythmic, funky, and head-nodding"},
    "Uplifting": {"description": "Hopeful, inspiring, and triumphant"},
    "Relaxed": {"description": "Calm, feel-good, and easy-going"},
    "Romantic": {"description": "Warm, intimate, and tender"},
    "Melancholic": {"description": "Sad, reflective, and bittersweet"},
    "Dark": {"description": "Brooding, moody, and ominous"},
    "Dreamy": {"description": "Atmospheric, floating, and ethereal"},
}


def classify_universal_mood(
    energy,
    valence,
    danceability,
    acousticness,
    instrumentalness,
    tempo_norm,
    loudness_norm,
    speechiness,
    liveness,
    mode,
    genre_family="Pop",
):
    """Rule-based mood classification with genre affinity bias."""
    scores = {
        "Aggressive": 0,
        "Euphoric": 0,
        "Energetic": 0,
        "Groovy": 0,
        "Uplifting": 0,
        "Relaxed": 0,
        "Romantic": 0,
        "Melancholic": 0,
        "Dark": 0,
        "Dreamy": 0,
    }

    if energy > 0.75 and valence < 0.35:
        scores["Aggressive"] += 10
    elif energy > 0.6 and valence < 0.3:
        scores["Aggressive"] += 6

    if energy > 0.7 and valence > 0.65 and danceability > 0.6:
        scores["Euphoric"] += 10
    elif energy > 0.6 and valence > 0.6:
        scores["Euphoric"] += 5

    if energy > 0.7:
        scores["Energetic"] += 8
    elif energy > 0.55:
        scores["Energetic"] += 4

    if danceability > 0.7 and valence > 0.45:
        scores["Groovy"] += 10
    elif danceability > 0.65 and valence > 0.4:
        scores["Groovy"] += 5

    if valence > 0.6 and mode == 1:
        scores["Uplifting"] += 8
    elif valence > 0.55 and mode == 1:
        scores["Uplifting"] += 4

    if energy < 0.5 and valence > 0.4:
        scores["Relaxed"] += 10
    elif energy < 0.55 and valence > 0.35:
        scores["Relaxed"] += 5

    if energy < 0.5 and acousticness > 0.3 and valence > 0.2:
        scores["Romantic"] += 10
    elif energy < 0.55 and acousticness > 0.15:
        scores["Romantic"] += 4

    if valence < 0.35 and energy < 0.5:
        scores["Melancholic"] += 10
    elif valence < 0.4 and energy < 0.55:
        scores["Melancholic"] += 5

    if valence < 0.3 and energy > 0.4 and mode == 0:
        scores["Dark"] += 10
    elif valence < 0.35 and mode == 0:
        scores["Dark"] += 5

    if (instrumentalness > 0.3 or acousticness > 0.5) and energy < 0.6:
        scores["Dreamy"] += 10
    elif acousticness > 0.3 and energy < 0.5:
        scores["Dreamy"] += 4

    genre_bias = {
        "Metal/Hardcore": {"Aggressive": 4, "Dark": 3},
        "Electronic/EDM": {"Energetic": 3, "Euphoric": 2},
        "Jazz/Soul": {"Groovy": 4, "Romantic": 3},
        "Classical/Ambient": {"Dreamy": 4, "Melancholic": 3},
        "Folk/Country": {"Relaxed": 3, "Romantic": 3},
        "Latin/World": {"Groovy": 3, "Euphoric": 3},
        "R&B/Funk": {"Groovy": 4, "Romantic": 3},
        "Hip-Hop/Rap": {"Dark": 3, "Aggressive": 2},
        "Rock/Alternative": {"Energetic": 2, "Dark": 2},
        "Pop": {},
    }
    for mood, bonus in genre_bias.get(genre_family, {}).items():
        scores[mood] += bonus

    return max(scores, key=scores.get)


GENRE_FAMILY_MAP = {
    "anime": "Pop",
    "cantopop": "Pop",
    "dance": "Pop",
    "disney": "Pop",
    "happy": "Pop",
    "indie-pop": "Pop",
    "j-idol": "Pop",
    "j-pop": "Pop",
    "k-pop": "Pop",
    "mandopop": "Pop",
    "party": "Pop",
    "pop": "Pop",
    "pop-film": "Pop",
    "power-pop": "Pop",
    "synth-pop": "Pop",
    "hip-hop": "Hip-Hop/Rap",
    "dancehall": "Hip-Hop/Rap",
    "reggaeton": "Hip-Hop/Rap",
    "breakbeat": "Electronic/EDM",
    "chicago-house": "Electronic/EDM",
    "club": "Electronic/EDM",
    "deep-house": "Electronic/EDM",
    "detroit-techno": "Electronic/EDM",
    "disco": "Electronic/EDM",
    "drum-and-bass": "Electronic/EDM",
    "dub": "Electronic/EDM",
    "dubstep": "Electronic/EDM",
    "edm": "Electronic/EDM",
    "electro": "Electronic/EDM",
    "electronic": "Electronic/EDM",
    "garage": "Electronic/EDM",
    "hardstyle": "Electronic/EDM",
    "house": "Electronic/EDM",
    "idm": "Electronic/EDM",
    "minimal-techno": "Electronic/EDM",
    "progressive-house": "Electronic/EDM",
    "techno": "Electronic/EDM",
    "trance": "Electronic/EDM",
    "trip-hop": "Electronic/EDM",
    "alt-rock": "Rock/Alternative",
    "alternative": "Rock/Alternative",
    "british": "Rock/Alternative",
    "emo": "Rock/Alternative",
    "goth": "Rock/Alternative",
    "grunge": "Rock/Alternative",
    "guitar": "Rock/Alternative",
    "indie": "Rock/Alternative",
    "psych-rock": "Rock/Alternative",
    "punk": "Rock/Alternative",
    "punk-rock": "Rock/Alternative",
    "rock": "Rock/Alternative",
    "rock-n-roll": "Rock/Alternative",
    "rockabilly": "Rock/Alternative",
    "ska": "Rock/Alternative",
    "jazz": "Jazz/Soul",
    "soul": "Jazz/Soul",
    "groove": "Jazz/Soul",
    "ambient": "Classical/Ambient",
    "chill": "Classical/Ambient",
    "classical": "Classical/Ambient",
    "new-age": "Classical/Ambient",
    "opera": "Classical/Ambient",
    "piano": "Classical/Ambient",
    "sleep": "Classical/Ambient",
    "study": "Classical/Ambient",
    "afrobeat": "Latin/World",
    "brazil": "Latin/World",
    "forro": "Latin/World",
    "french": "Latin/World",
    "german": "Latin/World",
    "indian": "Latin/World",
    "iranian": "Latin/World",
    "j-dance": "Latin/World",
    "j-rock": "Latin/World",
    "latin": "Latin/World",
    "latino": "Latin/World",
    "malay": "Latin/World",
    "mpb": "Latin/World",
    "pagode": "Latin/World",
    "salsa": "Latin/World",
    "samba": "Latin/World",
    "sertanejo": "Latin/World",
    "spanish": "Latin/World",
    "swedish": "Latin/World",
    "tango": "Latin/World",
    "turkish": "Latin/World",
    "world-music": "Latin/World",
    "black-metal": "Metal/Hardcore",
    "death-metal": "Metal/Hardcore",
    "grindcore": "Metal/Hardcore",
    "hard-rock": "Metal/Hardcore",
    "hardcore": "Metal/Hardcore",
    "heavy-metal": "Metal/Hardcore",
    "industrial": "Metal/Hardcore",
    "metal": "Metal/Hardcore",
    "metalcore": "Metal/Hardcore",
    "acoustic": "Pop",
    "bluegrass": "Folk/Country",
    "blues": "Folk/Country",
    "children": "Pop",
    "comedy": "Pop",
    "country": "Folk/Country",
    "folk": "Folk/Country",
    "gospel": "Folk/Country",
    "honky-tonk": "Folk/Country",
    "kids": "Pop",
    "romance": "R&B/Funk",
    "sad": "Pop",
    "show-tunes": "Pop",
    "singer-songwriter": "Folk/Country",
    "songwriter": "Folk/Country",
    "funk": "R&B/Funk",
    "r-n-b": "R&B/Funk",
    "reggae": "R&B/Funk",
}

GENRE_SUB_MOODS = {
    "Pop": {
        "Bubblegum Pop": {
            "description": "Playful, bright, and sugary sweet",
            "weights": {"valence": 2.0, "energy": 2.0, "danceability": 1.5, "mode": 1.5, "tempo": 1.0},
        },
        "Chill Pop": {
            "description": "Relaxed, feel-good, and easy-going",
            "weights": {"valence": 2.0, "energy": -2.5, "tempo": -1.5, "loudness_norm": -1.0, "danceability": 0.5},
        },
        "Dance Pop": {
            "description": "Upbeat, rhythmic, and club-ready",
            "weights": {"energy": 3.0, "danceability": 2.0, "tempo": 2.0, "loudness_norm": 1.0},
        },
        "Melancholy Pop": {
            "description": "Bittersweet, emotional, and reflective",
            "weights": {"valence": -3.0, "energy": -1.5, "mode": -2.0, "acousticness": 1.0},
        },
        "Anthemic Pop": {
            "description": "Soaring, triumphant, and stadium-sized",
            "weights": {"energy": 3.0, "liveness": 2.0, "tempo": 2.0, "valence": 1.0, "loudness_norm": 1.0},
        },
    },
    "Hip-Hop/Rap": {
        "Trap": {
            "description": "Dark, bass-heavy, and hard-hitting",
            "weights": {"energy": 1.5, "speechiness": 1.5, "loudness_norm": 1.0, "valence": -1.0},
        },
        "Boom Bap": {
            "description": "Classic, lyrical, and groove-driven",
            "weights": {"speechiness": 2.0, "danceability": 1.0, "energy": -0.5, "acousticness": 0.5},
        },
        "Feel-Good Rap": {
            "description": "Uplifting, party-oriented, and vibrant",
            "weights": {"valence": 2.0, "danceability": 1.5, "energy": 1.0, "speechiness": 0.5},
        },
        "Conscious Hip-Hop": {
            "description": "Thoughtful, introspective, and message-driven",
            "weights": {"speechiness": 2.0, "acousticness": 1.0, "energy": -0.5, "valence": -0.5},
        },
        "Hype Rap": {
            "description": "Aggressive, loud, and adrenaline-fueled",
            "weights": {"energy": 2.0, "loudness_norm": 2.0, "tempo": 1.0, "speechiness": 1.0},
        },
    },
    "Electronic/EDM": {
        "Peak-Time Rave": {
            "description": "Explosive, relentless, and euphoric",
            "weights": {"energy": 2.0, "tempo": 1.5, "loudness_norm": 1.5, "danceability": 1.0},
        },
        "Deep & Hypnotic": {
            "description": "Minimal, repetitive, and trance-inducing",
            "weights": {"instrumentalness": 2.0, "danceability": 1.0, "energy": -0.5, "speechiness": -1.0},
        },
        "Melodic Electronic": {
            "description": "Lush, emotional, and synth-driven",
            "weights": {"valence": 1.0, "instrumentalness": 1.5, "energy": 0.5, "mode": 1.0},
        },
        "Dark Electronic": {
            "description": "Brooding, industrial, and menacing",
            "weights": {"valence": -2.0, "mode": -1.5, "energy": 1.0, "loudness_norm": 1.0},
        },
        "Downtempo Chill": {
            "description": "Relaxed, ambient, and laid-back",
            "weights": {"energy": -2.0, "tempo": -1.5, "acousticness": 1.0, "instrumentalness": 1.0},
        },
    },
    "Rock/Alternative": {
        "Garage Fury": {
            "description": "Raw, distorted, and rebellious",
            "weights": {"energy": 2.0, "loudness_norm": 2.0, "valence": -0.5, "acousticness": -1.5},
        },
        "Indie Shimmer": {
            "description": "Jangly, bright, and wistful",
            "weights": {"valence": 1.0, "acousticness": 1.0, "energy": -0.5, "mode": 1.0},
        },
        "Post-Punk Gloom": {
            "description": "Cold, angular, and darkly atmospheric",
            "weights": {"valence": -2.0, "mode": -1.5, "energy": 0.5, "acousticness": -0.5},
        },
        "Arena Rock": {
            "description": "Anthemic, powerful, and crowd-rousing",
            "weights": {"energy": 1.5, "loudness_norm": 1.5, "liveness": 1.5, "valence": 1.0},
        },
        "Acoustic Rock": {
            "description": "Stripped-back, warm, and organic",
            "weights": {"acousticness": 2.0, "energy": -1.5, "instrumentalness": 0.5, "loudness_norm": -1.0},
        },
    },
    "Jazz/Soul": {
        "Smoky Lounge": {
            "description": "Intimate, late-night, and sultry",
            "weights": {"acousticness": 1.5, "energy": -1.5, "instrumentalness": 1.0, "valence": -0.5},
        },
        "Uptown Swing": {
            "description": "Swinging, joyful, and rhythmically infectious",
            "weights": {"valence": 2.0, "danceability": 1.5, "tempo": 1.0, "energy": 0.5},
        },
        "Free Jazz Chaos": {
            "description": "Experimental, dissonant, and unpredictable",
            "weights": {"instrumentalness": 2.0, "energy": 1.5, "valence": -1.0, "liveness": 1.0},
        },
        "Neo-Soul Warmth": {
            "description": "Smooth, warm, and groove-centered",
            "weights": {"danceability": 1.5, "acousticness": 1.0, "valence": 1.0, "energy": -0.5},
        },
        "Melancholy Ballad": {
            "description": "Sorrowful, tender, and deeply emotional",
            "weights": {"valence": -2.0, "energy": -1.5, "mode": -1.5, "acousticness": 1.0},
        },
    },
    "Classical/Ambient": {
        "Serene Meditation": {
            "description": "Peaceful, still, and deeply calming",
            "weights": {"energy": -2.0, "instrumentalness": 2.0, "acousticness": 1.5, "loudness_norm": -1.5},
        },
        "Dramatic Orchestral": {
            "description": "Grand, sweeping, and emotionally intense",
            "weights": {"energy": 2.0, "loudness_norm": 1.5, "instrumentalness": 1.0, "valence": -0.5},
        },
        "Pastoral Beauty": {
            "description": "Light, airy, and nature-inspired",
            "weights": {"valence": 1.5, "acousticness": 1.5, "instrumentalness": 1.0, "mode": 1.0},
        },
        "Dark Minimalism": {
            "description": "Sparse, haunting, and tension-filled",
            "weights": {"valence": -2.0, "mode": -1.5, "instrumentalness": 1.5, "energy": -1.0},
        },
        "Cosmic Drift": {
            "description": "Spacious, evolving, and otherworldly",
            "weights": {"instrumentalness": 2.0, "acousticness": -1.0, "energy": -1.0, "speechiness": -1.5},
        },
    },
    "Latin/World": {
        "Fiesta Caliente": {
            "description": "Hot, rhythmic, and dance-floor-ready",
            "weights": {"danceability": 2.0, "energy": 1.5, "valence": 1.5, "tempo": 1.0},
        },
        "Tropical Sunset": {
            "description": "Warm, breezy, and laid-back",
            "weights": {"valence": 1.5, "danceability": 1.0, "acousticness": 1.0, "energy": -0.5},
        },
        "Ritual & Roots": {
            "description": "Earthy, percussive, and spiritually grounded",
            "weights": {"acousticness": 2.0, "liveness": 1.5, "instrumentalness": 1.0, "speechiness": -1.0},
        },
        "Urban Latin": {
            "description": "Modern, bass-driven, and street-influenced",
            "weights": {"energy": 1.5, "danceability": 1.5, "loudness_norm": 1.0, "acousticness": -1.5},
        },
        "Saudade": {
            "description": "Nostalgic, longing, and bittersweet",
            "weights": {"valence": -2.0, "mode": -1.0, "acousticness": 1.5, "energy": -1.0},
        },
    },
    "Metal/Hardcore": {
        "Berserker Rage": {
            "description": "Blast-beat fury, extreme, and unrelenting",
            "weights": {"energy": 2.0, "loudness_norm": 2.0, "tempo": 1.5, "valence": -1.0},
        },
        "Doom & Sludge": {
            "description": "Slow, crushing, and oppressively heavy",
            "weights": {"tempo": -2.0, "loudness_norm": 1.5, "energy": 1.0, "valence": -1.5},
        },
        "Melodic Metal": {
            "description": "Soaring, harmonic, and technically precise",
            "weights": {"valence": 1.0, "energy": 1.5, "mode": 1.5, "instrumentalness": 1.0},
        },
        "Industrial Grind": {
            "description": "Mechanical, abrasive, and dystopian",
            "weights": {"energy": 1.5, "acousticness": -2.0, "valence": -1.5, "speechiness": 0.5},
        },
        "Epic Power Metal": {
            "description": "Triumphant, fast, and fantasy-inspired",
            "weights": {"tempo": 2.0, "energy": 1.5, "valence": 1.5, "mode": 1.0},
        },
    },
    "Folk/Country": {
        "Front Porch": {
            "description": "Warm, acoustic, and homespun",
            "weights": {"acousticness": 2.0, "valence": 1.0, "energy": -1.0, "instrumentalness": 0.5},
        },
        "Outlaw Country": {
            "description": "Gritty, rebellious, and road-worn",
            "weights": {"energy": 1.5, "valence": -0.5, "acousticness": 0.5, "loudness_norm": 1.0},
        },
        "Heartbreak Ballad": {
            "description": "Tearful, slow, and deeply personal",
            "weights": {"valence": -2.0, "energy": -1.5, "mode": -1.5, "acousticness": 1.0},
        },
        "Hoedown Stomp": {
            "description": "Fast, lively, and foot-stomping",
            "weights": {"tempo": 2.0, "danceability": 1.5, "energy": 1.5, "valence": 1.0},
        },
        "Campfire Reverie": {
            "description": "Quiet, contemplative, and starlit",
            "weights": {"acousticness": 2.0, "energy": -2.0, "instrumentalness": 1.0, "loudness_norm": -1.5},
        },
    },
    "R&B/Funk": {
        "Bedroom R&B": {
            "description": "Sensual, smooth, and intimate",
            "weights": {"danceability": 1.0, "energy": -1.0, "valence": 0.5, "acousticness": 1.0},
        },
        "Funk Strut": {
            "description": "Groovy, bass-heavy, and irresistibly rhythmic",
            "weights": {"danceability": 2.0, "energy": 1.5, "valence": 1.5, "tempo": 0.5},
        },
        "Neon Nights": {
            "description": "Synth-laced, nocturnal, and polished",
            "weights": {"danceability": 1.5, "acousticness": -1.5, "energy": 0.5, "valence": 0.5},
        },
        "Heartache R&B": {
            "description": "Vulnerable, slow-burning, and emotionally raw",
            "weights": {"valence": -2.0, "energy": -1.5, "mode": -1.0, "speechiness": 0.5},
        },
        "Party Funk": {
            "description": "Explosive, celebratory, and horn-driven",
            "weights": {"energy": 2.0, "valence": 2.0, "danceability": 1.5, "liveness": 1.0},
        },
    },
}
