"""Real-time sales event generation for the Monogram streaming layer.

Events mirror the SALES_DATA shape used by the batch direct ingester but carry a
real-time ``event_ts`` and, when available, reference real product/customer/store
IDs sampled from ``data/*.json`` so they keep referential integrity with the dbt
dimensions. Falls back to synthetic IDs when the reference files are absent, so
the producer always runs (e.g. in CI or before data generation).
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CHANNELS = ["Online VIP", "Boutique", "Showroom privé", "Téléphone"]
COUNTRIES = ["France", "Italy", "United Kingdom", "United States", "Japan"]
BRANDS = ["Chanel", "Dior", "Yves Saint Laurent", "Hermès", "Prada", "Gucci", "Versace", "Valentino"]
CATEGORIES = ["Robe", "Veste", "Pantalon", "Jupe", "Chemise", "Manteau", "Blouse", "Accessoire"]

# Business fields carried on the wire (Kafka message value), lowercase keys.
EVENT_FIELDS = [
    "event_id", "sale_id", "event_ts", "sale_date", "customer_id", "product_id",
    "product_name", "quantity", "unit_price", "total_amount", "channel", "store_id", "country",
]


def _load_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


@dataclass
class ReferencePools:
    """Pools of real IDs sampled by the event factory to preserve FK integrity."""

    products: list[dict[str, Any]]
    customer_ids: list[str]
    stores: list[dict[str, Any]]

    @classmethod
    def load(cls, data_dir: Path | str = "data") -> ReferencePools:
        data_dir = Path(data_dir)
        products = _load_ndjson(data_dir / "products.json")
        customers = _load_ndjson(data_dir / "customers.json")
        stores = _load_ndjson(data_dir / "stores.json")

        if not products:
            products = [
                {"product_id": f"P{2001 + i}",
                 "name": f"{random.choice(BRANDS)} {random.choice(CATEGORIES)}",
                 "price": round(random.uniform(200, 2500), 2)}
                for i in range(50)
            ]
        if not customers:
            customers = [{"customer_id": f"C{1001 + i}"} for i in range(100)]
        if not stores:
            stores = [{"store_id": f"ST{3001 + i}", "country": random.choice(COUNTRIES)} for i in range(5)]

        return cls(
            products=[
                {"product_id": p["product_id"],
                 "name": p.get("name", "Vintage piece"),
                 "price": float(p.get("price") or round(random.uniform(200, 2500), 2))}
                for p in products
            ],
            customer_ids=[c["customer_id"] for c in customers],
            stores=[
                {"store_id": s["store_id"], "country": s.get("country") or random.choice(COUNTRIES)}
                for s in stores
            ],
        )


class SalesEventFactory:
    """Produces real-time sales events sampling the reference pools."""

    def __init__(self, pools: ReferencePools | None = None, data_dir: Path | str = "data"):
        self.pools = pools or ReferencePools.load(data_dir)

    def make(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(timezone.utc)
        product = random.choice(self.pools.products)
        store = random.choice(self.pools.stores)
        quantity = random.randint(1, 3)
        unit_price = round(float(product["price"]), 2)
        total_amount = round(quantity * unit_price, 2)
        return {
            "event_id": uuid.uuid4().hex,
            "sale_id": "S" + uuid.uuid4().hex[:12].upper(),
            "event_ts": now.isoformat(),
            "sale_date": now.date().isoformat(),
            "customer_id": random.choice(self.pools.customer_ids),
            "product_id": product["product_id"],
            "product_name": product["name"],
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "channel": random.choice(CHANNELS),
            "store_id": store["store_id"],
            "country": store.get("country") or random.choice(COUNTRIES),
        }
