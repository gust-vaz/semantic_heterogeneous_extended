import pytest
from benchmarks.harness.profiles import PROFILES, DEFAULT_PROFILE, get_profile

REQUIRED_KEYS = {"records", "clients", "warmup_s", "measure_s", "reps", "real_max_files"}


def test_default_profile_is_small():
    assert DEFAULT_PROFILE == "small"


def test_every_profile_has_all_keys():
    for name, values in PROFILES.items():
        assert set(values) == REQUIRED_KEYS, f"profile {name} has wrong keys"


def test_smoke_is_the_cheapest_profile():
    assert PROFILES["smoke"]["records"] < PROFILES["small"]["records"]
    assert PROFILES["small"]["records"] < PROFILES["full"]["records"]


def test_get_profile_returns_a_copy():
    a = get_profile("small")
    a["records"] = 1
    assert get_profile("small")["records"] != 1


def test_get_profile_applies_overrides():
    p = get_profile("small", clients=16)
    assert p["clients"] == 16
    assert p["records"] == PROFILES["small"]["records"]


def test_get_profile_ignores_none_overrides():
    p = get_profile("small", clients=None)
    assert p["clients"] == PROFILES["small"]["clients"]


def test_get_profile_rejects_unknown_name():
    with pytest.raises(ValueError) as exc:
        get_profile("enormous")
    assert "enormous" in str(exc.value)


def test_get_profile_rejects_unknown_override_key():
    with pytest.raises(ValueError):
        get_profile("small", bogus=3)


def test_full_profile_loads_all_real_files():
    assert PROFILES["full"]["real_max_files"] is None
