"""Shared pytest fixtures for the Monogram Paris ETL test suite."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from monogram_etl.generators.data_generator import DataGenerator, GenerationConfig


@pytest.fixture
def small_config(tmp_path: Path) -> GenerationConfig:
    """Tiny configuration that keeps generation under 1 second."""
    return GenerationConfig(
        sales=20,
        products=10,
        customers=15,
        suppliers=3,
        stores=2,
        promotions=2,
        returns=3,
        reviews=5,
        inventory=4,
        output_dir=tmp_path,
    )


@pytest.fixture
def generator(small_config: GenerationConfig) -> DataGenerator:
    return DataGenerator(small_config)


@pytest.fixture
def generated_dataset(tmp_path: Path, small_config: GenerationConfig) -> dict[str, list[dict]]:
    """Run the full generator into tmp_path and return parsed NDJSON for every entity."""
    DataGenerator(small_config).generate_all()

    files = {
        "suppliers": tmp_path / "suppliers.json",
        "products": tmp_path / "products.json",
        "customers": tmp_path / "customers.json",
        "stores": tmp_path / "stores.json",
        "sales": tmp_path / "sales.json",
        "returns": tmp_path / "returns.json",
        "reviews": tmp_path / "reviews.json",
        "inventory": tmp_path / "inventory.json",
        "promotions": tmp_path / "promotions.json",
    }

    parsed: dict[str, list[dict]] = {}
    for name, path in files.items():
        if not path.exists():
            parsed[name] = []
            continue
        with path.open(encoding="utf-8") as fh:
            parsed[name] = [json.loads(line) for line in fh if line.strip()]
    return parsed
