"""monogram-stream-produce: stream synthetic real-time sales events to Kafka/Redpanda.

Continuously generates Monogram sales events and publishes them to the sales
topic at a configurable rate. The Kafka key is ``store_id`` so a store's events
land on one partition (ordered per store). Use ``--dry-run`` to print events
without a broker (no confluent-kafka needed), handy for local checks and CI.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import time

from dotenv import load_dotenv

from monogram_etl.streaming.config import KafkaSettings
from monogram_etl.streaming.events import SalesEventFactory

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("monogram.stream.producer")

_RUNNING = True


def _stop(signum, frame):  # noqa: ANN001, ARG001
    global _RUNNING
    _RUNNING = False
    logger.info("Shutdown signal received; flushing and exiting...")


def run(*, rate: float, count: int, settings: KafkaSettings, data_dir: str, dry_run: bool) -> int:
    factory = SalesEventFactory(data_dir=data_dir)
    interval = 1.0 / rate if rate > 0 else 0.0
    sent = 0

    producer = None
    if not dry_run:
        from confluent_kafka import Producer  # lazy import so --dry-run needs no broker lib

        producer = Producer(
            {"bootstrap.servers": settings.bootstrap_servers, "linger.ms": 50, "enable.idempotence": True}
        )

    def _delivery(err, msg):  # noqa: ANN001
        if err is not None:
            logger.error("Delivery failed: %s", err)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    logger.info(
        "Producing to %s (topic=%s) at ~%.1f eps%s",
        settings.bootstrap_servers, settings.topic, rate, " [DRY RUN]" if dry_run else "",
    )

    start = time.monotonic()
    while _RUNNING and (count == 0 or sent < count):
        event = factory.make()
        payload = json.dumps(event).encode("utf-8")
        if dry_run:
            print(payload.decode("utf-8"))
        else:
            assert producer is not None  # set above whenever dry_run is False
            producer.produce(settings.topic, key=event["store_id"].encode(), value=payload, callback=_delivery)
            producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            logger.info("Produced %d events (%.1f eps)", sent, sent / max(1e-6, time.monotonic() - start))
        if interval:
            time.sleep(interval)

    if producer is not None:
        producer.flush(10)
    logger.info("Done. Produced %d events.", sent)
    return sent


def main() -> None:
    p = argparse.ArgumentParser(description="Stream real-time Monogram sales events to Kafka/Redpanda")
    p.add_argument("--rate", type=float, default=5.0, help="Events per second (default 5)")
    p.add_argument("--count", type=int, default=0, help="Events to send (0 = run until stopped)")
    p.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers (overrides env)")
    p.add_argument("--topic", default=None, help="Kafka topic (overrides env)")
    p.add_argument("--data-dir", default="data", help="Reference data dir for sampling IDs")
    p.add_argument("--dry-run", action="store_true", help="Print events instead of publishing (no broker)")
    args = p.parse_args()

    settings = KafkaSettings.from_env()
    settings = KafkaSettings(
        bootstrap_servers=args.bootstrap or settings.bootstrap_servers,
        topic=args.topic or settings.topic,
        group_id=settings.group_id,
    )
    run(rate=args.rate, count=args.count, settings=settings, data_dir=args.data_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
