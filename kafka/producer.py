"""
Kafka Producer — World Happiness Streaming Pipeline
Workshop 3: ETL with Apache Kafka and Machine Learning

Reads the unified CSV dataset and streams records one-by-one
to the Kafka topic 'happiness-predictions' as JSON events.
"""

import json
import time
import logging
import argparse
from pathlib import Path
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ────────────────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent.parent
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME      = "happiness-predictions"
UNIFIED_CSV     = str(BASE_DIR / "data" / "processed" / "unified_happiness.csv")
DEFAULT_DELAY   = 0.5   # seconds between messages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Producer factory ─────────────────────────────────────────────────────────
def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                # wait for leader + all ISR replicas
        retries=3,
        linger_ms=0,               # send immediately (no batching delay)
    )


# ── Record → Kafka event ─────────────────────────────────────────────────────
def row_to_event(row: pd.Series) -> dict:
    """
    Maps a unified dataset row to the required Kafka JSON schema.

    Required JSON format (per workshop spec):
    {
        "country": str,
        "year": int,
        "gdp": float,
        "family": float,
        "health": float,
        "freedom": float,
        "generosity": float,
        "corruption": float,
        "actual_happiness_score": float
    }
    """
    return {
        "country":                str(row["country"]),
        "year":                   int(row["year"]),
        "gdp":                    round(float(row["gdp_per_capita"]), 6),
        "family":                 round(float(row["social_support"]), 6),
        "health":                 round(float(row["health_life_expectancy"]), 6),
        "freedom":                round(float(row["freedom"]), 6),
        "generosity":             round(float(row["generosity"]), 6),
        "corruption":             round(float(row["corruption"]), 6),
        "actual_happiness_score": round(float(row["happiness_score"]), 6),
    }


# ── Delivery callback ────────────────────────────────────────────────────────
def on_send_success(record_metadata, event):
    log.info(
        "Sent → topic=%s | partition=%s | offset=%s | country=%s year=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
        event["country"],
        event["year"],
    )


def on_send_error(exc):
    log.error("Failed to send message: %s", exc)


# ── Main ─────────────────────────────────────────────────────────────────────
def main(delay: float, max_records: int | None, bootstrap: str) -> None:
    log.info("Loading unified dataset from: %s", UNIFIED_CSV)
    df = pd.read_csv(UNIFIED_CSV)

    # Drop rows with any NaN in required columns (skip dirty records here)
    required = [
        "country", "year", "gdp_per_capita", "social_support",
        "health_life_expectancy", "freedom", "generosity",
        "corruption", "happiness_score",
    ]
    df_clean = df[required].dropna().reset_index(drop=True)

    if max_records:
        df_clean = df_clean.head(max_records)

    log.info("Total records to stream: %d", len(df_clean))
    log.info("Connecting to Kafka at: %s", bootstrap)

    producer = build_producer(bootstrap)
    log.info("Producer connected. Streaming to topic: '%s'", TOPIC_NAME)

    sent = 0
    errors = 0

    for _, row in df_clean.iterrows():
        event = row_to_event(row)
        try:
            future = producer.send(TOPIC_NAME, value=event)
            future.add_callback(on_send_success, event=event)
            future.add_errback(on_send_error)
            sent += 1
        except KafkaError as e:
            log.error("KafkaError for %s/%s: %s", event["country"], event["year"], e)
            errors += 1

        time.sleep(delay)

    producer.flush()
    producer.close()

    log.info("Streaming complete. Sent: %d | Errors: %d", sent, errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Happiness Kafka Producer")
    parser.add_argument("--delay",       type=float, default=DEFAULT_DELAY,
                        help="Seconds between messages (default: 0.5)")
    parser.add_argument("--max-records", type=int,   default=None,
                        help="Limit number of records to send (default: all)")
    parser.add_argument("--bootstrap",   type=str,   default=KAFKA_BOOTSTRAP,
                        help="Kafka bootstrap server (default: localhost:9092)")
    args = parser.parse_args()

    main(delay=args.delay, max_records=args.max_records, bootstrap=args.bootstrap)
