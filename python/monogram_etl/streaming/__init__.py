"""Real-time streaming layer for the Monogram pipeline.

Producer streams synthetic sales events to Kafka/Redpanda; the consumer ingests
them into Snowflake via Snowpipe Streaming (high-performance Python SDK). This is
the real-time path that complements the existing batch ingestion.
"""
