"""Named sizing profiles.

Sizes are fixed data, never derived from the machine, so numbers stay
comparable across machines. `records` sizes the synthetic corpus;
`real_max_files` sizes the real DATASUS corpus (None = load every file).
"""

PROFILES = {
    "smoke": {"records": 1_000, "clients": 2, "warmup_s": 2,
              "measure_s": 5, "reps": 1, "real_max_files": 1},
    "small": {"records": 20_000, "clients": 4, "warmup_s": 5,
              "measure_s": 20, "reps": 3, "real_max_files": 3},
    "full": {"records": 200_000, "clients": 8, "warmup_s": 10,
             "measure_s": 60, "reps": 5, "real_max_files": None},
}

DEFAULT_PROFILE = "small"


def get_profile(name, **overrides):
    """Return a copy of the named profile with any non-None overrides applied."""
    if name not in PROFILES:
        raise ValueError(
            f"Unknown profile '{name}'. Choose: {', '.join(PROFILES)}"
        )
    profile = dict(PROFILES[name])
    for key, value in overrides.items():
        if key not in profile:
            raise ValueError(
                f"Unknown profile override '{key}'. Valid: {', '.join(profile)}"
            )
        if value is not None:
            profile[key] = value
    return profile
