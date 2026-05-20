"""Unit tests for the Faker-based data generator."""
from __future__ import annotations

from pathlib import Path

import pytest

from monogram_etl.generators.data_generator import DataGenerator, GenerationConfig

pytestmark = pytest.mark.unit


def test_id_ranges_match_configured_counts(small_config: GenerationConfig) -> None:
    gen = DataGenerator(small_config)

    assert gen.ranges["customer"] == (1001, 1001 + small_config.customers - 1)
    assert gen.ranges["product"] == (2001, 2001 + small_config.products - 1)
    assert gen.ranges["store"] == (3001, 3001 + small_config.stores - 1)
    assert gen.ranges["sale"] == (100001, 100001 + small_config.sales - 1)


def test_auto_computed_supplier_count_when_zero() -> None:
    cfg = GenerationConfig(products=200, suppliers=0, output_dir=Path("./_unused"))
    gen = DataGenerator(cfg)
    # generator auto-fills: max(3, products // 20) = max(3, 10) = 10
    assert cfg.suppliers == 10
    assert gen.ranges["supplier"] == (1, 10)


def test_id_range_iterator_yields_full_range(generator: DataGenerator) -> None:
    customer_ids = list(generator.id_range_iterator("customer"))
    assert customer_ids[0] == 1001
    assert customer_ids[-1] == 1001 + generator.config.customers - 1
    assert len(customer_ids) == generator.config.customers


def test_random_id_from_range_stays_in_bounds(generator: DataGenerator) -> None:
    start, end = generator.ranges["product"]
    for _ in range(50):
        assert start <= generator.random_id_from_range("product") <= end


def test_generate_suppliers_yields_expected_count(generator: DataGenerator) -> None:
    suppliers = list(generator.generate_suppliers())
    assert len(suppliers) == generator.config.suppliers
    for s in suppliers:
        assert s["supplier_id"].startswith("SUP")
        assert s["name"]
        assert 0 < s["quality_rating"] <= 5


def test_generate_products_have_positive_economics(generator: DataGenerator) -> None:
    for p in generator.generate_products():
        assert p["price"] > 0
        assert p["cost"] > 0
        assert p["price"] >= p["cost"], f"price < cost on {p['product_id']}"


def test_generate_customers_have_required_columns(generator: DataGenerator) -> None:
    required = {
        "customer_id", "first_name", "last_name", "email", "phone",
        "segment", "registration_date", "lifetime_value", "marketing_consent",
    }
    for c in generator.generate_customers():
        missing = required - c.keys()
        assert not missing, f"customer is missing columns: {missing}"
