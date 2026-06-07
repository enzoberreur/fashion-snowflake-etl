-- 03_raw_streaming.sql
-- Real-time streaming layer: target table for Snowpipe Streaming (high-performance).
-- The default pipe "STREAM_SALES-STREAMING" is auto-created at ingest time with
-- MATCH_BY_COLUMN_NAME, so no explicit CREATE PIPE is needed. The streaming
-- consumer (python/monogram_etl/streaming/consumer.py) appends rows whose keys
-- match these columns. EVENT_TS is the event (sale) time; INGESTED_AT is when the
-- consumer landed it - their difference is the end-to-end streaming latency.

USE ROLE INGEST;
USE WAREHOUSE INGEST;
USE DATABASE INGEST;
USE SCHEMA INGEST;

CREATE TABLE IF NOT EXISTS STREAM_SALES (
    EVENT_ID        VARCHAR(36),
    SALE_ID         VARCHAR(20),
    EVENT_TS        TIMESTAMP_NTZ,
    SALE_DATE       DATE,
    CUSTOMER_ID     VARCHAR(20),
    PRODUCT_ID      VARCHAR(20),
    PRODUCT_NAME    VARCHAR(200),
    QUANTITY        NUMBER(10,0),
    UNIT_PRICE      NUMBER(12,2),
    TOTAL_AMOUNT    NUMBER(14,2),
    CHANNEL         VARCHAR(30),
    STORE_ID        VARCHAR(20),
    COUNTRY         VARCHAR(50),
    KAFKA_PARTITION NUMBER(10,0),
    KAFKA_OFFSET    NUMBER(20,0),
    INGESTED_AT     TIMESTAMP_NTZ
);
