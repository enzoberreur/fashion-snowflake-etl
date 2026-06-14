"""Unit tests for the streaming event factory (no external dependencies)."""
from __future__ import annotations

import pytest
from monogram_etl.streaming.events import EVENT_FIELDS, ReferencePools, SalesEventFactory

pytestmark = pytest.mark.unit


def _pools() -> ReferencePools:
    return ReferencePools(
        products=[{"product_id": "P2001", "name": "Chanel Robe", "price": 1200.0}],
        customer_ids=["C1001", "C1002"],
        stores=[{"store_id": "ST3001", "country": "France"}],
    )


def test_event_has_all_fields():
    event = SalesEventFactory(_pools()).make()
    assert set(EVENT_FIELDS) <= set(event)
    assert all(event[f] is not None for f in EVENT_FIELDS)


def test_total_amount_is_quantity_times_price():
    event = SalesEventFactory(_pools()).make()
    assert event["total_amount"] == round(event["quantity"] * event["unit_price"], 2)


def test_ids_sampled_from_reference_pools():
    event = SalesEventFactory(_pools()).make()
    assert event["product_id"] == "P2001"
    assert event["customer_id"] in {"C1001", "C1002"}
    assert event["store_id"] == "ST3001"


def test_reference_pools_fallback_when_data_missing(tmp_path):
    pools = ReferencePools.load(tmp_path)  # empty dir -> synthetic fallback
    assert pools.products and pools.customer_ids and pools.stores


def test_event_ids_are_unique():
    factory = SalesEventFactory(_pools())
    ids = {factory.make()["event_id"] for _ in range(200)}
    assert len(ids) == 200
