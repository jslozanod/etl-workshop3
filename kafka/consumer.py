"""
Kafka Consumer — World Happiness Streaming Pipeline
Workshop 3: ETL with Apache Kafka and Machine Learning

For each Kafka message the consumer:
  1. Deserializes the JSON event
  2. Stores the raw event in raw_happiness_events (always, before any validation)
  3. Validates the event schema and values
  4. If valid: loads model.pkl, generates a prediction, stores result in fact_predictions
  5. If invalid: marks the raw record accordingly, skips prediction, pipeline keeps running
"""

import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Configuration ────────────────────────────────────────────────────────────
BASE_DIR         = Path(__file__).resolve().parent.parent
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9092")
TOPIC_NAME       = "happiness-predictions"
GROUP_ID         = "happiness-consumer-group"
MODEL_PATH       = str(BASE_DIR / "models" / "model.pkl")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5434")),
    "dbname":   os.getenv("DB_NAME",     "happiness_db"),
    "user":     os.getenv("DB_USER",     "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_password"),
}

# Feature order must exactly match training order
REQUIRED_FEATURES = [
    "gdp_per_capita",
    "social_support",
    "health_life_expectancy",
    "freedom",
    "generosity",
    "corruption",
]

# Kafka field name → unified model feature name
FIELD_MAP = {
    "gdp":        "gdp_per_capita",
    "family":     "social_support",
    "health":     "health_life_expectancy",
    "freedom":    "freedom",
    "generosity": "generosity",
    "corruption": "corruption",
}

REQUIRED_FIELDS = [
    "country", "year", "gdp", "family", "health",
    "freedom", "generosity", "corruption", "actual_happiness_score",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Database helpers ─────────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_or_create_country(cur, country_name: str) -> int:
    cur.execute(
        "INSERT INTO dim_country (country_name) VALUES (%s) ON CONFLICT (country_name) DO NOTHING RETURNING country_id",
        (country_name,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (country_name,))
    return cur.fetchone()[0]


def get_or_create_date(cur, year: int) -> int:
    cur.execute(
        "INSERT INTO dim_date (year) VALUES (%s) ON CONFLICT (year) DO NOTHING RETURNING date_id",
        (year,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT date_id FROM dim_date WHERE year = %s", (year,))
    return cur.fetchone()[0]


def insert_raw_event(cur, event: dict, raw_payload: str,
                     status: str, message: str | None = None) -> int:
    cur.execute(
        """
        INSERT INTO raw_happiness_events
            (country, year, gdp, family, health, freedom, generosity, corruption,
             actual_happiness_score, raw_payload, processing_status, validation_message)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING raw_event_id
        """,
        (
            event.get("country"),
            event.get("year"),
            event.get("gdp"),
            event.get("family"),
            event.get("health"),
            event.get("freedom"),
            event.get("generosity"),
            event.get("corruption"),
            event.get("actual_happiness_score"),
            raw_payload,
            status,
            message,
        ),
    )
    return cur.fetchone()[0]


def insert_dim_raw_event(cur, raw_event_id: int, event: dict) -> int:
    cur.execute(
        """
        INSERT INTO dim_raw_event
            (raw_event_id, country, year, gdp, family, health, freedom,
             generosity, corruption, actual_happiness_score,
             processing_status, received_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'VALID', NOW())
        RETURNING dim_raw_event_id
        """,
        (
            raw_event_id,
            event.get("country"),
            event.get("year"),
            event.get("gdp"),
            event.get("family"),
            event.get("health"),
            event.get("freedom"),
            event.get("generosity"),
            event.get("corruption"),
            event.get("actual_happiness_score"),
        ),
    )
    return cur.fetchone()[0]


def insert_prediction(cur, raw_event_id: int, dim_raw_event_id: int,
                      country_id: int, date_id: int,
                      actual: float, predicted: float, features: dict) -> None:
    cur.execute(
        """
        INSERT INTO fact_predictions
            (raw_event_id, dim_raw_event_id, country_id, date_id,
             actual_score, predicted_score,
             gdp_per_capita, social_support, health_life_expectancy,
             freedom, generosity, corruption)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            raw_event_id,
            dim_raw_event_id,
            country_id,
            date_id,
            actual,
            predicted,
            features["gdp_per_capita"],
            features["social_support"],
            features["health_life_expectancy"],
            features["freedom"],
            features["generosity"],
            features["corruption"],
        ),
    )


# ── Validation ───────────────────────────────────────────────────────────────
def validate_event(event: dict) -> tuple[bool, str, str]:
    """
    Returns (is_valid, status_code, message).
    status_code: VALID | INVALID_SCHEMA | INVALID_VALUES
    """
    # 1. Missing fields
    missing = [f for f in REQUIRED_FIELDS if f not in event]
    if missing:
        return False, "INVALID_SCHEMA", f"Missing fields: {missing}"

    # 2. Type validation
    try:
        str(event["country"])
        int(event["year"])
    except (ValueError, TypeError) as e:
        return False, "INVALID_SCHEMA", f"Type error in country/year: {e}"

    numeric_fields = ["gdp", "family", "health", "freedom", "generosity",
                      "corruption", "actual_happiness_score"]
    for field in numeric_fields:
        val = event.get(field)
        if val is None:
            return False, "INVALID_SCHEMA", f"Null value in numeric field: {field}"
        try:
            float(val)
        except (ValueError, TypeError):
            return False, "INVALID_SCHEMA", f"Non-numeric value in {field}: {val}"

    # 3. Value range validation
    if not (0 <= float(event["gdp"]) <= 3):
        return False, "INVALID_VALUES", f"gdp out of range [0,3]: {event['gdp']}"
    if not (0 <= float(event["actual_happiness_score"]) <= 10):
        return False, "INVALID_VALUES", f"happiness_score out of range [0,10]: {event['actual_happiness_score']}"
    if not (1900 <= int(event["year"]) <= 2100):
        return False, "INVALID_VALUES", f"year out of range: {event['year']}"

    return True, "VALID", None


# ── Model loader ─────────────────────────────────────────────────────────────
_model_cache = None

def load_model():
    global _model_cache
    if _model_cache is None:
        log.info("Loading model from: %s", MODEL_PATH)
        _model_cache = joblib.load(MODEL_PATH)
        log.info("Model loaded successfully.")
    return _model_cache


def predict(event: dict) -> tuple[float, dict]:
    model = load_model()
    features = {unified: float(event[raw]) for raw, unified in FIELD_MAP.items()}
    X = pd.DataFrame([features])[REQUIRED_FEATURES]
    predicted = float(model.predict(X)[0])
    return predicted, features


# ── Main consumer loop ───────────────────────────────────────────────────────
running = True

def handle_shutdown(signum, frame):
    global running
    log.info("Shutdown signal received. Finishing gracefully...")
    running = False

signal.signal(signal.SIGINT,  handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def main() -> None:
    log.info("Connecting to Kafka at %s, topic: %s", KAFKA_BOOTSTRAP, TOPIC_NAME)
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    log.info("Connecting to PostgreSQL: %s@%s/%s",
             DB_CONFIG["user"], DB_CONFIG["host"], DB_CONFIG["dbname"])
    conn = get_db_connection()
    conn.autocommit = False
    log.info("Database connected. Waiting for events...")

    stats = {"total": 0, "valid": 0, "invalid_schema": 0,
             "invalid_values": 0, "prediction_error": 0}

    while running:
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    stats["total"] += 1
                    event       = msg.value
                    raw_payload = json.dumps(event)

                    log.info(
                        "Received #%d | country=%s year=%s",
                        stats["total"],
                        event.get("country", "?"),
                        event.get("year", "?"),
                    )

                    with conn.cursor() as cur:
                        # ── STEP 1: Store raw event immediately ──────────────
                        is_valid, status, validation_msg = validate_event(event)
                        raw_event_id = insert_raw_event(
                            cur, event, raw_payload, status, validation_msg
                        )
                        conn.commit()

                        if not is_valid:
                            stats[status.lower()] = stats.get(status.lower(), 0) + 1
                            log.warning(
                                "Invalid event stored (raw_id=%d) status=%s msg=%s",
                                raw_event_id, status, validation_msg,
                            )
                            continue

                        # ── STEP 2: Generate prediction ───────────────────────
                        try:
                            predicted, features = predict(event)
                            actual = float(event["actual_happiness_score"])

                            dim_raw_event_id = insert_dim_raw_event(cur, raw_event_id, event)
                            country_id       = get_or_create_country(cur, event["country"])
                            date_id          = get_or_create_date(cur, int(event["year"]))

                            insert_prediction(
                                cur, raw_event_id, dim_raw_event_id,
                                country_id, date_id,
                                actual, predicted, features,
                            )
                            conn.commit()
                            stats["valid"] += 1

                            log.info(
                                "Prediction stored | country=%s year=%s | actual=%.3f predicted=%.3f error=%.3f",
                                event["country"], event["year"],
                                actual, predicted, abs(actual - predicted),
                            )

                        except Exception as pred_err:
                            conn.rollback()
                            log.error("Prediction error for raw_id=%d: %s", raw_event_id, pred_err)
                            with conn.cursor() as upd:
                                upd.execute(
                                    "UPDATE raw_happiness_events SET processing_status=%s, validation_message=%s WHERE raw_event_id=%s",
                                    ("PREDICTION_ERROR", str(pred_err), raw_event_id),
                                )
                            conn.commit()
                            stats["prediction_error"] += 1

        except KafkaError as ke:
            log.error("Kafka error: %s", ke)
            time.sleep(2)
        except Exception as e:
            log.error("Unexpected error: %s", e)
            conn.rollback()

    log.info("Consumer stopped. Final stats: %s", stats)
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
