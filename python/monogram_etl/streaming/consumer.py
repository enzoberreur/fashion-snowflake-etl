"""monogram-stream-consume: consume sales events from Kafka and stream into Snowflake.

One Snowpipe Streaming channel per Kafka partition; the Kafka offset is the
channel's offset token, which gives exactly-once ingestion with replay on
restart - on partition assignment we resume each channel from its last committed
offset and seek Kafka accordingly. Logs throughput, committed offsets and lag.
Use --dry-run to consume and print without writing to Snowflake.
"""
from __future__ import annotations

import argparse
import json
import logging
import signal
import time

from dotenv import load_dotenv

from monogram_etl.streaming.config import KafkaSettings, SnowpipeSettings
from monogram_etl.streaming.sink import SnowpipeStreamingSink, event_to_row

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger("monogram.stream.consumer")

_RUNNING = True


def _stop(signum, frame):  # noqa: ANN001, ARG001
    global _RUNNING
    _RUNNING = False
    logger.info("Shutdown signal received; flushing and closing channels...")


def run(
    settings: KafkaSettings,
    snow: SnowpipeSettings,
    *,
    batch_size: int,
    flush_interval: float,
    max_messages: int,
    dry_run: bool,
    prefix: str,
) -> int:
    from confluent_kafka import Consumer

    sinks: dict[int, SnowpipeStreamingSink | None] = {}

    def get_sink(partition: int) -> SnowpipeStreamingSink | None:
        if partition not in sinks:
            if dry_run:
                sinks[partition] = None
            else:
                s = SnowpipeStreamingSink(
                    snow,
                    client_name=f"monogram-consumer-p{partition}",
                    channel_name=f"{prefix}-p{partition}",
                    profile_path=f"profile-p{partition}.json",
                )
                s.open()
                sinks[partition] = s
        return sinks[partition]

    def on_assign(consumer, partitions):  # noqa: ANN001
        # Resume each partition from the last offset committed to Snowflake (exactly-once).
        for tp in partitions:
            s = get_sink(tp.partition)
            token = s.latest_committed_offset() if s else None
            if token is not None and token != "":
                tp.offset = int(token) + 1
        consumer.assign(partitions)
        logger.info("Assigned partitions (resume offsets): %s",
                    [(tp.partition, tp.offset) for tp in partitions])

    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": settings.group_id,
        "enable.auto.commit": False,      # Snowpipe offset token is the source of truth
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([settings.topic], on_assign=on_assign)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    logger.info("Consuming %s from %s (group=%s)%s", settings.topic, settings.bootstrap_servers,
                settings.group_id, " [DRY RUN]" if dry_run else "")

    consumed = 0
    pending = 0
    last_flush = time.monotonic()
    last_offset: dict[int, int] = {}
    start = time.monotonic()

    def flush_all() -> None:
        for p, s in sinks.items():
            if s is not None:
                s.flush()
                logger.info("partition %d: committed offset=%s (last seen=%s)",
                            p, s.latest_committed_offset(), last_offset.get(p))

    try:
        while _RUNNING and (max_messages == 0 or consumed < max_messages):
            msg = consumer.poll(1.0)
            if msg is None:
                if pending and (time.monotonic() - last_flush) >= flush_interval:
                    flush_all()
                    pending, last_flush = 0, time.monotonic()
                continue
            if msg.error():
                logger.warning("consumer error: %s", msg.error())
                continue

            raw, p, off = msg.value(), msg.partition(), msg.offset()
            # Non-error messages always carry a value, partition and offset.
            assert raw is not None and p is not None and off is not None
            event = json.loads(raw)
            row = event_to_row(event, partition=p, offset=off)
            if dry_run:
                print(json.dumps(row, default=str))
            else:
                sink = get_sink(p)
                assert sink is not None  # sinks are real whenever dry_run is False
                sink.append(row, offset_token=off)
            last_offset[p] = off
            consumed += 1
            pending += 1

            if pending >= batch_size or (time.monotonic() - last_flush) >= flush_interval:
                flush_all()
                pending, last_flush = 0, time.monotonic()
            if consumed % 100 == 0:
                logger.info("consumed %d (%.1f eps)", consumed, consumed / max(1e-6, time.monotonic() - start))
    finally:
        if pending:
            flush_all()
        for s in sinks.values():
            if s is not None:
                s.close()
        consumer.close()

    logger.info("Done. Consumed %d events.", consumed)
    return consumed


def main() -> None:
    p = argparse.ArgumentParser(description="Consume Monogram sales events from Kafka into Snowflake (Snowpipe Streaming)")
    p.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers (overrides env)")
    p.add_argument("--topic", default=None, help="Kafka topic (overrides env)")
    p.add_argument("--group", default=None, help="Consumer group (overrides env)")
    p.add_argument("--batch-size", type=int, default=50, help="Flush to Snowflake every N messages")
    p.add_argument("--flush-interval", type=float, default=5.0, help="Max seconds between flushes")
    p.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 = run until stopped)")
    p.add_argument("--channel-prefix", default=None, help="Channel name prefix (overrides env)")
    p.add_argument("--dry-run", action="store_true", help="Consume and print without writing to Snowflake")
    args = p.parse_args()

    settings = KafkaSettings.from_env()
    settings = KafkaSettings(
        bootstrap_servers=args.bootstrap or settings.bootstrap_servers,
        topic=args.topic or settings.topic,
        group_id=args.group or settings.group_id,
    )
    snow = SnowpipeSettings.from_env()
    prefix = args.channel_prefix or snow.channel_prefix
    run(settings, snow, batch_size=args.batch_size, flush_interval=args.flush_interval,
        max_messages=args.max_messages, dry_run=args.dry_run, prefix=prefix)


if __name__ == "__main__":
    main()
