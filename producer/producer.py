#!/usr/bin/env python3
"""
Production Mock Event Producer and Test Runner
Real-Time Transaction Anomaly & Dynamic Rule Engine
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fraud-producer")

DEFAULT_BOOTSTRAP = "localhost:29092"
TOPIC_TRANSACTIONS = "transactions"
TOPIC_RULES = "fraud-rules"
TOPIC_ALERTS = "fraud-alerts"
TOPIC_DLQ = "transactions-dlq"

MERCHANT_CATEGORIES = [
    "RETAIL",
    "ELECTRONICS",
    "LUXURY_GOODS",
    "GAMBLING",
    "CRYPTO_EXCHANGE",
    "TRAVEL",
    "GROCERY",
    "RESTAURANT",
]

LOCATIONS = [
    "US-NYC",
    "US-SFO",
    "US-MIA",
    "UK-LON",
    "JP-TYO",
    "DE-BER",
    "SG-SIN",
    "AE-DXB",
]


def get_producer(bootstrap_servers, retries=10, delay=3):
    """Wait for Kafka brokers to be ready and initialize producer."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info("Successfully connected to Kafka brokers at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logger.warning("Attempt %d/%d: Brokers not ready yet at %s. Retrying in %ds...",
                           attempt, retries, bootstrap_servers, delay)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers}")


def generate_transaction(account_id=None, amount=None, event_time=None, category=None, location=None):
    now_ms = int(time.time() * 1000)
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id or f"ACC_{random.randint(1001, 1020)}",
        "amount": round(amount if amount is not None else random.uniform(10.0, 350.0), 2),
        "event_time": event_time if event_time is not None else now_ms,
        "merchant_category": category or random.choice(MERCHANT_CATEGORIES),
        "location": location or random.choice(LOCATIONS),
    }


def emit_rule(producer, rule_id, rule_type, threshold, window_minutes=None, is_active=True):
    rule = {
        "rule_id": rule_id,
        "rule_type": rule_type,
        "threshold": float(threshold),
        "window_minutes": int(window_minutes) if window_minutes else 0,
        "is_active": bool(is_active),
    }
    logger.info("Publishing Rule -> %s", json.dumps(rule))
    future = producer.send(TOPIC_RULES, key=rule_id, value=rule)
    producer.flush()
    record_metadata = future.get(timeout=10)
    logger.info("Rule '%s' sent to partition %d offset %d",
                rule_id, record_metadata.partition, record_metadata.offset)


def run_stream(producer, rate_per_sec=5, duration_sec=None):
    logger.info("Starting continuous transaction stream (Rate: %d tx/sec)...", rate_per_sec)
    start_time = time.time()
    count = 0
    try:
        while True:
            if duration_sec and (time.time() - start_time) > duration_sec:
                break
            tx = generate_transaction()
            # Random occasional anomalous transaction (5% chance)
            if random.random() < 0.05:
                tx["amount"] = round(random.uniform(5500.0, 9500.0), 2)
                tx["merchant_category"] = "CRYPTO_EXCHANGE"

            producer.send(TOPIC_TRANSACTIONS, key=tx["account_id"], value=tx)
            count += 1
            if count % 20 == 0:
                logger.info("Streamed %d transactions (latest: $%.2f for %s)", count, tx["amount"], tx["account_id"])
            time.sleep(1.0 / rate_per_sec)
    except KeyboardInterrupt:
        logger.info("Stream interrupted by user.")
    finally:
        producer.flush()
        logger.info("Stream ended. Total transactions emitted: %d", count)


def run_inject_late(producer, skew_seconds=45, count=3):
    logger.info("Injecting %d late transactions with skew of -%d seconds...", count, skew_seconds)
    now_ms = int(time.time() * 1000)
    late_time = now_ms - (skew_seconds * 1000)

    for i in range(count):
        tx = generate_transaction(
            account_id=f"ACC_LATE_{i % 2}",
            amount=150.0 + (i * 10),
            event_time=late_time,
            category="RETAIL",
        )
        logger.info("Emitting late tx %d/%d: id=%s, event_time=%d (current=%d, delay=%ds)",
                    i + 1, count, tx["transaction_id"], tx["event_time"], now_ms, skew_seconds)
        producer.send(TOPIC_TRANSACTIONS, key=tx["account_id"], value=tx)

    producer.flush()
    logger.info("Finished injecting late transactions. Check '%s' topic.", TOPIC_DLQ)


def run_monitor(bootstrap_servers, timeout_sec=None):
    logger.info("Starting real-time monitor on topics: '%s' and '%s'...", TOPIC_ALERTS, TOPIC_DLQ)
    consumer = KafkaConsumer(
        TOPIC_ALERTS,
        TOPIC_DLQ,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"monitor-group-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    start_time = time.time()
    try:
        for message in consumer:
            if message.topic == TOPIC_ALERTS:
                alert = message.value
                logger.warning(
                    "🚨 [ALERT DETECTED] Rule: %s | Type: %s | Account: %s | TriggerValue: %s | Details: %s",
                    alert.get("rule_id"),
                    alert.get("rule_type"),
                    alert.get("account_id"),
                    alert.get("trigger_value"),
                    alert.get("details"),
                )
            elif message.topic == TOPIC_DLQ:
                tx = message.value
                logger.info(
                    "📦 [DLQ DETECTED] Late Tx: %s | Account: %s | EventTime: %s | Amount: $%.2f",
                    tx.get("transaction_id"),
                    tx.get("account_id"),
                    datetime.fromtimestamp(tx.get("event_time", 0) / 1000, tz=timezone.utc).isoformat(),
                    tx.get("amount", 0.0),
                )
            if timeout_sec and (time.time() - start_time) > timeout_sec:
                break
    except KeyboardInterrupt:
        logger.info("Monitor stopped.")
    finally:
        consumer.close()


def run_demo(producer, bootstrap_servers):
    logger.info("=================================================================")
    logger.info("🚀 STARTING AUTOMATED END-TO-END DEMO & VALIDATION WORKFLOW")
    logger.info("=================================================================")

    # Step 1: Initialize Baseline Fraud Rules
    logger.info("\n--- STEP 1: Broadcasting Baseline Fraud Rules ---")
    emit_rule(producer, "RULE_SINGLE_HIGH", "SINGLE_TRANSACTION_THRESHOLD", 5000.0, is_active=True)
    emit_rule(producer, "RULE_BURST_RATE", "HIGH_FREQUENCY_BURST", 3.0, window_minutes=2, is_active=True)
    emit_rule(producer, "RULE_ACCUMULATION", "SLIDING_WINDOW_ACCUMULATION", 8000.0, window_minutes=5, is_active=True)
    time.sleep(2)

    # Step 2: Normal Stream Transactions
    logger.info("\n--- STEP 2: Streaming Normal Traffic (Below Thresholds) ---")
    for _ in range(8):
        tx = generate_transaction(account_id="ACC_NORMAL_01", amount=random.uniform(50.0, 300.0))
        producer.send(TOPIC_TRANSACTIONS, key=tx["account_id"], value=tx)
        time.sleep(0.3)
    producer.flush()
    logger.info("8 normal transactions emitted. (No alerts expected)")

    # Step 3: Trigger Single High Value Rule
    logger.info("\n--- STEP 3: Ingesting Anomalous Transaction ($6,750 > $5,000) ---")
    fraud_tx = generate_transaction(
        account_id="ACC_FRAUD_01",
        amount=6750.00,
        category="CRYPTO_EXCHANGE",
        location="AE-DXB",
    )
    logger.info("Emitting high-value tx: id=%s for account=%s amount=$%.2f",
                fraud_tx["transaction_id"], fraud_tx["account_id"], fraud_tx["amount"])
    producer.send(TOPIC_TRANSACTIONS, key=fraud_tx["account_id"], value=fraud_tx)
    producer.flush()

    # Step 4: Trigger High Frequency Burst Rule
    logger.info("\n--- STEP 4: Triggering Rapid Burst (4 transactions in < 2 seconds) ---")
    burst_account = "ACC_BURST_02"
    for i in range(4):
        tx = generate_transaction(account_id=burst_account, amount=120.0 + (i * 10))
        producer.send(TOPIC_TRANSACTIONS, key=burst_account, value=tx)
        time.sleep(0.2)
    producer.flush()
    logger.info("Burst emitted for %s (> 3 allowed)", burst_account)

    # Step 5: Advance Watermark with In-Order Heartbeats
    logger.info("\n--- STEP 5: Advancing Pipeline Watermarks ---")
    for i in range(25):
        tx = generate_transaction(account_id=f"ACC_SYNC_{i % 2}", amount=25.0)
        producer.send(TOPIC_TRANSACTIONS, key=tx["account_id"], value=tx)
        time.sleep(0.3)
    producer.flush()

    # Step 6: Inject Late-Arriving Data into Watermarked Pipeline
    logger.info("\n--- STEP 6: Injecting Late-Arriving Records (> 10s Out-Of-Order) ---")
    run_inject_late(producer, skew_seconds=50, count=2)

    # Step 7: Demonstrate Zero-Downtime Dynamic Rule Update
    logger.info("\n--- STEP 7: Demonstrating Dynamic Rule Update at Runtime ---")
    logger.info("Updating RULE_SINGLE_HIGH threshold from $5,000.00 -> $2,000.00 without restarting Flink job...")
    emit_rule(producer, "RULE_SINGLE_HIGH", "SINGLE_TRANSACTION_THRESHOLD", 2000.0, is_active=True)
    time.sleep(3)

    logger.info("Sending a $2,800.00 transaction (previously below $5,000, now violates $2,000 rule)...")
    tx_update_test = generate_transaction(
        account_id="ACC_DYNAMIC_TEST",
        amount=2800.00,
        category="LUXURY_GOODS",
    )
    producer.send(TOPIC_TRANSACTIONS, key=tx_update_test["account_id"], value=tx_update_test)
    producer.flush()

    logger.info("=================================================================")
    logger.info("✅ DEMO WORKFLOW EMISSION COMPLETE!")
    logger.info("Checking results in topics '%s' and '%s'...", TOPIC_ALERTS, TOPIC_DLQ)
    logger.info("=================================================================")


def main():
    parser = argparse.ArgumentParser(description="Real-Time Transaction Anomaly & Dynamic Rule Engine Producer")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP})")
    parser.add_argument("--mode", choices=["stream", "rule", "late", "monitor", "demo"], default="demo",
                        help="Execution mode (default: demo)")
    parser.add_argument("--rate", type=int, default=5, help="Transaction emission rate per second (for stream mode)")
    parser.add_argument("--duration", type=int, default=None, help="Duration in seconds (for stream mode)")
    parser.add_argument("--rule-id", type=str, default="RULE_MANUAL_1", help="Rule ID for rule mode")
    parser.add_argument("--rule-type", type=str, default="SINGLE_TRANSACTION_THRESHOLD",
                        choices=["SINGLE_TRANSACTION_THRESHOLD", "SLIDING_WINDOW_ACCUMULATION", "HIGH_FREQUENCY_BURST"],
                        help="Rule type for rule mode")
    parser.add_argument("--threshold", type=float, default=5000.0, help="Rule threshold value")
    parser.add_argument("--window", type=int, default=5, help="Window duration in minutes")
    parser.add_argument("--active", type=lambda v: v.lower() in ("true", "1", "yes"), default=True, help="Is rule active")
    parser.add_argument("--skew", type=int, default=45, help="Delay skew in seconds for late mode")
    parser.add_argument("--count", type=int, default=3, help="Record count for late mode")

    args = parser.parse_args()

    if args.mode == "monitor":
        run_monitor(args.bootstrap)
        return

    producer = get_producer(args.bootstrap)

    if args.mode == "stream":
        run_stream(producer, rate_per_sec=args.rate, duration_sec=args.duration)
    elif args.mode == "rule":
        emit_rule(producer, args.rule_id, args.rule_type, args.threshold, args.window, args.active)
    elif args.mode == "late":
        run_inject_late(producer, skew_seconds=args.skew, count=args.count)
    elif args.mode == "demo":
        run_demo(producer, args.bootstrap)


if __name__ == "__main__":
    main()
