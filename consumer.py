# consumer.py — FIXED METRICS (acc/prec/rec working properly)

import json, threading, csv, os, time, sys, traceback
from kafka import KafkaConsumer
from river import tree, preprocessing, metrics
from datetime import datetime

BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"
ANOMALIES_CSV = "detected_anomalies.csv"

# -------------------------
# MODEL + METRICS
# -------------------------
model = preprocessing.StandardScaler() | tree.HoeffdingTreeClassifier()
acc = metrics.Accuracy()
prec = metrics.Precision()
rec = metrics.Recall()

detecting = False
consumer_thread = None
kafka_consumer = None

# -------------------------
# Write CSV header
# -------------------------
if not os.path.exists(ANOMALIES_CSV):
    with open(ANOMALIES_CSV, "w", newline="") as f:
        csv.writer(f).writerow([
            "timestamp","transaction_id","user_id","amount","user_score",
            "predicted","label","components","merchant","payment_method",
            "location","implied_speed_kmh"
        ])

# -------------------------
# Feature extraction
# -------------------------
def extract_features(txn):
    try:
        return {
            "amount": float(txn.get("amount", 0)),
            "txn_per_day": float(txn.get("txn_per_day", 0)),
            "fraud_history": float(txn.get("fraud_history", 0)),
            "user_score": float(txn.get("user_score", 0)),
            "merchant_risk": float(txn.get("score_components", {}).get("merchant_risk", 0)),
            "payment_risk": float(txn.get("score_components", {}).get("payment_risk", 0)),
        }
    except:
        return None

# -------------------------
# Create consumer
# -------------------------
def _create_consumer():
    global kafka_consumer
    try:
        kafka_consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[BOOTSTRAP],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=1500,
            group_id="fraud-detector-group"
        )
        print("✅ Kafka consumer connected.")
        return True
    except Exception as e:
        print("⚠️ Kafka connect failed:", e)
        return False


# -------------------------
# MAIN DETECTION LOOP
# -------------------------
def detect_loop():
    global detecting, kafka_consumer
    attempt = 0

    while detecting:
        try:
            # connect once
            if kafka_consumer is None:
                if not _create_consumer():
                    attempt += 1
                    time.sleep(1.5)
                    continue

            print("🧠 Detection started...")

            for msg in kafka_consumer:
                if not detecting:
                    break

                txn = msg.value
                if not isinstance(txn, dict):
                    continue

                # history callback → interface
                cb = getattr(sys.modules.get("consumer"), "_history_callback", None)
                if cb:
                    try:
                        cb(txn)
                    except:
                        pass

                # -------- FEATURES ----------
                X = extract_features(txn)
                if X is None:
                    continue

                y_true = 1 if txn.get("is_anomaly_label") else 0

                # -------- FIXED PREDICTION ----------
                pred_raw = model.predict_one(X)

                if pred_raw is None:
                    y_pred = 0
                else:
                    y_pred = int(pred_raw)

                # -------- UPDATE METRICS ----------
                try:
                    acc.update(y_true, y_pred)
                    prec.update(y_true, y_pred)
                    rec.update(y_true, y_pred)
                except:
                    pass

                # -------- LEARN ----------
                model.learn_one(X, y_true)

                emoji = "🚨" if y_pred == 1 else "✅"

                print(
                    f"{emoji} [{datetime.now().strftime('%H:%M:%S')}] "
                    f"{txn.get('transaction_id')} | user={txn.get('user_id')} "
                    f"| amt={txn.get('amount')} | score={txn.get('user_score')} "
                    f"| pred={'ANOMALY' if y_pred else 'NORMAL'} "
                    f"| label={'ANOMALY' if y_true else 'NORMAL'} "
                    f"| acc={acc.get():.3f} prec={prec.get():.3f} rec={rec.get():.3f}"
                )

                # save anomalies
                if y_pred == 1 or y_true == 1:
                    comps = txn.get("score_components", {})
                    with open(ANOMALIES_CSV, "a", newline="") as f:
                        csv.writer(f).writerow([
                            datetime.now().isoformat(),
                            txn.get("transaction_id"),
                            txn.get("user_id"),
                            txn.get("amount"),
                            txn.get("user_score"),
                            y_pred,
                            y_true,
                            ";".join([f"{k}:{v}" for k,v in comps.items()]),
                            txn.get("merchant"),
                            txn.get("payment_method"),
                            txn.get("location"),
                            txn.get("implied_speed_kmh")
                        ])

            # timeout → reconnect silently
            kafka_consumer = None
            time.sleep(0.5)

        except Exception as e:
            print("❗ ERROR in detect loop:", e)
            print(traceback.format_exc())
            kafka_consumer = None
            time.sleep(1)

    print("🛑 Detection stopped.")
    kafka_consumer = None


# -------------------------
# Controls
# -------------------------
def START_DETECT():
    global detecting, consumer_thread
    if detecting:
        print("Detection already running.")
        return
    detecting = True
    consumer_thread = threading.Thread(target=detect_loop, daemon=True)
    consumer_thread.start()
    print("✅ START_DETECT called.")

def STOP_DETECT():
    global detecting, kafka_consumer
    detecting = False
    if kafka_consumer:
        try: kafka_consumer.close()
        except: pass
    print("⏹️ STOP_DETECT requested.")

def STOP_ALL():
    STOP_DETECT()
    try:
        from producer import STOP_STREAM
        STOP_STREAM()
    except:
        pass
    print("🛑 STOP_ALL executed.")

print("Consumer ready. Use START_DETECT() / STOP_DETECT() / STOP_ALL().")
