"""
Tests for the data quality expectation suite.
These run without Spark — pure Python unit tests.
"""

import sys

sys.path.insert(0, "quality/expectations")


def test_suite_has_critical_and_warning_sections():
    from silver_orders_suite import build_expectation_suite

    suite = build_expectation_suite()
    assert "critical" in suite
    assert "warning" in suite
    assert len(suite["critical"]) >= 4
    assert len(suite["warning"]) >= 2


def test_all_expectations_have_required_keys():
    from silver_orders_suite import build_expectation_suite

    suite = build_expectation_suite()
    required_keys = {"name", "description", "check", "metric_name", "metric_fn"}
    for section in ["critical", "warning"]:
        for exp in suite[section]:
            missing = required_keys - set(exp.keys())
            assert (
                not missing
            ), f"Expectation '{exp.get('name')}' missing keys: {missing}"


def test_expectation_names_are_unique():
    from silver_orders_suite import build_expectation_suite

    suite = build_expectation_suite()
    all_names = [e["name"] for e in suite["critical"] + suite["warning"]]
    assert len(all_names) == len(set(all_names)), "Duplicate expectation names found"
