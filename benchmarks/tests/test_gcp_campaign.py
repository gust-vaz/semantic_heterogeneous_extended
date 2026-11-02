"""The campaign scheduler's decisions, offline. No test here touches GCP."""
import importlib.util
import os

SPEC = importlib.util.spec_from_file_location(
    "gcp_campaign", os.path.join("deploy", "gcp", "campaign.py"))
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)


def test_every_deployment_counts_its_control_vm():
    assert {name: campaign.ips(name) for name in campaign.PLAN} == {
        "rs5": 6, "single": 2, "rs3": 4, "sh1": 2, "sh4": 5, "sh7": 8}


def test_the_plan_runs_every_experiment_of_both_series():
    for name in ("single", "rs3", "rs5"):
        assert campaign.PLAN[name] == ["b1", "b2", "b3", "b4"]
    for name in ("sh1", "sh4", "sh7"):
        assert campaign.PLAN[name] == ["s1", "s3", "s4", "s5"]   # s2 rides along with s1


def test_starts_as_many_as_fit_in_plan_order():
    assert campaign.fitting(list(campaign.PLAN), []) == ["rs5", "single"]


def test_a_small_deployment_fills_the_gap_a_large_one_cannot_use():
    assert campaign.fitting(["rs3", "sh1", "sh4", "sh7"], ["rs5"]) == ["sh1"]


def test_never_exceeds_the_ip_budget():
    assert campaign.fitting(["sh7"], ["single"]) == []
    assert campaign.fitting(["sh1", "sh7"], ["sh4"]) == ["sh1"]


def test_the_largest_cluster_runs_alone_once_everything_else_is_done():
    assert campaign.fitting(["sh7"], []) == ["sh7"]
