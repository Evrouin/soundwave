# Mood Classification System

## Overview

Soundwave uses a two-tier mood classification system that combines universal emotional moods with genre-specific sub-labels. Every track gets both a **universal mood** (how it feels) and a **genre sub-label** (what style it is within its genre family).

## Tier 1 — Universal Moods (10 labels)

Rule-based classification using audio features with genre family bias for borderline cases.

| Mood | Description | Audio Signals | Distribution |
|------|-------------|---------------|-------------|
| Energetic | High-energy, pumped, and powerful | energy > 0.7, high tempo | ~31% |
| Euphoric | Joyful, uplifting, peak happiness | energy > 0.7, valence > 0.65, danceability > 0.6 | ~14% |
| Groovy | Rhythmic, funky, and head-nodding | danceability > 0.7, valence > 0.45 | ~12% |
| Dreamy | Atmospheric, floating, and ethereal | high instrumentalness/acousticness, low energy | ~9% |
| Aggressive | Intense, angry, and raw | energy > 0.75, valence < 0.35 | ~9% |
| Relaxed | Calm, feel-good, and easy-going | energy < 0.5, valence > 0.4 | ~7% |
| Romantic | Warm, intimate, and tender | low energy, acoustic, moderate valence | ~7% |
| Melancholic | Sad, reflective, and bittersweet | valence < 0.35, energy < 0.5 | ~7% |
| Dark | Brooding, moody, and ominous | valence < 0.3, moderate energy, minor key | ~3% |
| Uplifting | Hopeful, inspiring, and triumphant | valence > 0.6, major key | ~2% |

### Genre Bias

Genre family provides a small scoring bonus to nudge borderline tracks toward genre-appropriate moods:

| Genre Family | Mood Affinity |
|---|---|
| Metal/Hardcore | +Aggressive, +Dark |
| Electronic/EDM | +Energetic, +Euphoric |
| Jazz/Soul | +Groovy, +Romantic |
| Classical/Ambient | +Dreamy, +Melancholic |
| Folk/Country | +Relaxed, +Romantic |
| Latin/World | +Groovy, +Euphoric |
| R&B/Funk | +Groovy, +Romantic |
| Hip-Hop/Rap | +Dark, +Aggressive |
| Rock/Alternative | +Energetic, +Dark |
| Pop | neutral (no bias) |

## Tier 2 — Genre Sub-Labels (50 labels)

Each of the 10 genre families has 5 sub-labels, assigned by weighted audio feature scoring within the family.

### Pop
| Sub-Label | Description |
|-----------|-------------|
| Bubblegum Pop | Playful, bright, and sugary sweet |
| Chill Pop | Relaxed, feel-good, and easy-going |
| Dance Pop | Upbeat, rhythmic, and club-ready |
| Melancholy Pop | Bittersweet, emotional, and reflective |
| Anthemic Pop | Soaring, triumphant, and stadium-sized |

### Hip-Hop/Rap
| Sub-Label | Description |
|-----------|-------------|
| Trap | Dark, bass-heavy, and hard-hitting |
| Boom Bap | Hard-hitting, classic, and gritty |
| Feel-Good Rap | Upbeat, positive, and party-ready |
| Conscious Hip-Hop | Intellectual, deep, and narrative-driven |
| Hype Rap | Aggressive, high-energy, and adrenaline-fueled |

### Electronic/EDM
| Sub-Label | Description |
|-----------|-------------|
| Peak-Time Rave | High-energy, driving, and euphoric |
| Deep & Hypnotic | Minimal, pulsing, and trance-inducing |
| Melodic Electronic | Emotional, layered, and uplifting |
| Dark Electronic | Industrial, brooding, and menacing |
| Downtempo Chill | Slow, atmospheric, and relaxing |

### Rock/Alternative
| Sub-Label | Description |
|-----------|-------------|
| Garage Fury | Raw, loud, and unpolished |
| Indie Shimmer | Bright, jangly, and introspective |
| Post-Punk Gloom | Dark, angular, and moody |
| Arena Rock | Anthemic, powerful, and crowd-rousing |
| Acoustic Rock | Stripped-back, warm, and organic |

### Jazz/Soul
| Sub-Label | Description |
|-----------|-------------|
| Smoky Lounge | Intimate, late-night, and sultry |
| Uptown Swing | Lively, brassy, and toe-tapping |
| Free Jazz Chaos | Experimental, dissonant, and unpredictable |
| Neo-Soul Warmth | Smooth, romantic, and groove-heavy |
| Melancholy Ballad | Tender, sorrowful, and deeply emotional |

### Classical/Ambient
| Sub-Label | Description |
|-----------|-------------|
| Serene Meditation | Peaceful, still, and centering |
| Dramatic Orchestral | Grand, sweeping, and cinematic |
| Pastoral Beauty | Gentle, nature-inspired, and flowing |
| Dark Minimalism | Sparse, unsettling, and contemplative |
| Cosmic Drift | Spacious, otherworldly, and expansive |

### Latin/World
| Sub-Label | Description |
|-----------|-------------|
| Fiesta Caliente | Hot, rhythmic, and celebratory |
| Tropical Sunset | Warm, breezy, and laid-back |
| Ritual & Roots | Earthy, traditional, and spiritual |
| Urban Latin | Modern, bass-driven, and street-smart |
| Saudade | Nostalgic, longing, and bittersweet |

### Metal/Hardcore
| Sub-Label | Description |
|-----------|-------------|
| Berserker Rage | Relentless, chaotic, and furious |
| Doom & Sludge | Slow, crushing, and oppressive |
| Melodic Metal | Harmonized, soaring, and powerful |
| Industrial Grind | Mechanical, abrasive, and relentless |
| Epic Power Metal | Triumphant, fast, and fantastical |

### Folk/Country
| Sub-Label | Description |
|-----------|-------------|
| Front Porch | Warm, acoustic, and homespun |
| Outlaw Country | Rebellious, twangy, and rough-edged |
| Heartbreak Ballad | Tender, sorrowful, and deeply personal |
| Hoedown Stomp | Fast, lively, and foot-stomping |
| Campfire Reverie | Quiet, reflective, and starlit |

### R&B/Funk
| Sub-Label | Description |
|-----------|-------------|
| Bedroom R&B | Intimate, slow, and sensual |
| Funk Strut | Groovy, bass-heavy, and confident |
| Neon Nights | Synth-driven, nocturnal, and smooth |
| Heartache R&B | Emotional, vulnerable, and raw |
| Party Funk | Upbeat, danceable, and celebratory |

## Genre Family Mapping

All 114 Spotify genres in the dataset are mapped to one of the 10 families. See `airflow/dags/mood_config.py` for the complete mapping.

## Audio Features Used

| Feature | Range | Description |
|---------|-------|-------------|
| danceability | 0.0–1.0 | How suitable for dancing |
| energy | 0.0–1.0 | Perceptual intensity and activity |
| valence | 0.0–1.0 | Musical positiveness (happy vs sad) |
| acousticness | 0.0–1.0 | Confidence the track is acoustic |
| instrumentalness | 0.0–1.0 | Likelihood of no vocals |
| tempo | BPM | Beats per minute (normalized to 0–1) |
| loudness | -60–0 dB | Overall volume (normalized to 0–1) |
| speechiness | 0.0–1.0 | Presence of spoken words |
| liveness | 0.0–1.0 | Probability of live performance |
| mode | 0 or 1 | Minor (0) or major (1) key |
