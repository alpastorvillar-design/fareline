from __future__ import annotations

import pytest

from fareline.m2 import contracts, types


@pytest.mark.parametrize("service", sorted(contracts.SERVICE_CONTRACTS))
def test_a_contract_declares_one_target_type_per_canonical_column(service: str) -> None:
    contract = contracts.SERVICE_CONTRACTS[service]
    names = [item.canonical_name for item in contract.columns]

    assert len(names) == len(set(names))
    for column in contract.columns:
        assert types.spark_sql_name(column.target_type)


@pytest.mark.parametrize("service", sorted(contracts.SERVICE_CONTRACTS))
def test_source_aliases_never_collide_across_canonical_columns(service: str) -> None:
    contract = contracts.SERVICE_CONTRACTS[service]
    seen: set[str] = set()

    for column in contract.columns:
        assert not seen & column.alias_keys, column.canonical_name
        seen |= column.alias_keys


def test_aliases_are_matched_case_insensitively() -> None:
    airport = contracts.YELLOW.column("airport_fee")

    assert airport.alias_keys == {"airport_fee"}
    assert "Airport_fee" in airport.source_aliases


def test_the_two_services_never_share_a_fare_component_meaning() -> None:
    yellow = set(contracts.YELLOW.fare_components)
    hvfhv = set(contracts.HVFHV.fare_components)

    # Only the two levies that upstream defines identically for both services
    # may carry the same name; nothing else is comparable.
    assert yellow & hvfhv == {"congestion_surcharge", "airport_fee", "cbd_congestion_fee"}
    assert "total_amount" not in hvfhv
    assert "driver_pay" not in yellow
    assert "base_passenger_fare" not in yellow


@pytest.mark.parametrize("service", sorted(contracts.SERVICE_CONTRACTS))
def test_every_declared_fare_component_is_a_contract_column(service: str) -> None:
    contract = contracts.SERVICE_CONTRACTS[service]

    for name in contract.fare_components:
        assert contract.column(name).target_type == types.FLOAT64


@pytest.mark.parametrize("service", sorted(contracts.SERVICE_CONTRACTS))
def test_the_rule_columns_a_contract_names_all_exist(service: str) -> None:
    contract = contracts.SERVICE_CONTRACTS[service]

    for name in (
        contract.pickup_timestamp,
        contract.dropoff_timestamp,
        contract.pickup_zone_key,
        contract.dropoff_zone_key,
    ):
        assert contract.column(name).required
    assert contract.column(contract.pickup_timestamp).target_type == types.TIMESTAMP_NTZ
    assert contract.column(contract.pickup_zone_key).role == "zone_key"


@pytest.mark.parametrize("service", sorted(contracts.SERVICE_CONTRACTS))
def test_every_row_rule_declares_a_known_action(service: str) -> None:
    contract = contracts.SERVICE_CONTRACTS[service]
    names = [rule.name for rule in contract.row_rules]

    assert len(names) == len(set(names))
    for rule in contract.row_rules:
        assert rule.action in {contracts.INCIDENT, contracts.QUARANTINE}
        assert rule.severity in {"low", "medium", "high"}
        assert rule.description


def test_only_impossible_rows_are_quarantined() -> None:
    quarantined = {
        rule.name for rule in contracts.YELLOW.row_rules if rule.action == contracts.QUARANTINE
    }

    # An out-of-period pickup is real upstream behaviour, not an impossible row,
    # so it is recorded and published rather than withheld.
    assert quarantined == {
        "pickup_timestamp_missing",
        "negative_duration",
        "guarded_promotion_out_of_range",
    }
    assert contracts.YELLOW.rule("pickup_out_of_declared_period").action == contracts.INCIDENT


def test_the_fingerprint_follows_the_contract_and_not_its_ordering() -> None:
    first = contracts.YELLOW.fingerprint

    assert first == contracts.YELLOW.fingerprint
    assert first != contracts.HVFHV.fingerprint

    changed = contracts.ServiceContract(
        **{
            **contracts.YELLOW.__dict__,
            "columns": (
                *contracts.YELLOW.columns[:-1],
                contracts.ColumnContract(
                    "cbd_congestion_fee", ("cbd_congestion_fee",), types.FLOAT64, True, "measure"
                ),
            ),
        }
    )
    assert changed.fingerprint != first


def test_local_time_is_declared_and_never_inferred() -> None:
    for contract in contracts.SERVICE_CONTRACTS.values():
        assert contract.local_time_zone == "America/New_York"
        assert contract.rule("local_time_ambiguous").action == contracts.INCIDENT
        assert contract.rule("local_time_nonexistent").action == contracts.INCIDENT
