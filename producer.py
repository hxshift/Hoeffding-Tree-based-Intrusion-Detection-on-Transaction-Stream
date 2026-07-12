# producer.py
# ---------- PRODUCER. Use START_STREAM() / STOP_STREAM() ----------
import json, time, random, threading
from datetime import datetime, timezone, timedelta
from collections import deque
from math import radians, sin, cos, sqrt, asin
from kafka import KafkaProducer

# ---- Config (tweak as needed) ----
BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"

# You can tune these for demo:
RATE_SECONDS = 1.0              # interval between messages (default 1.0s)
ANOMALY_INJECTION_RATE = 0.30   # ~30% injected anomalies (adjustable)

# ---- Kafka producer (safe serializer) ----
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP],
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    retries=5
)

# ---- City coordinates for velocity checks ----
CITY_COORDS = {
    "Delhi": (28.7041, 77.1025), "Mumbai": (19.0760, 72.8777),
    "Bangalore": (12.9716, 77.5946), "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639), "Hyderabad": (17.3850, 78.4867),
    "Pune": (18.5204, 73.8567), "Ahmedabad": (23.0225, 72.5714),
    "London": (51.5074, -0.1278), "New York": (40.7128, -74.0060),
    "Paris": (48.8566, 2.3522), "Singapore": (1.3521, 103.8198)
}
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1); dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

# ---- Categories & merchants (rich list for demo) ----
CATEGORIES = {
    "Groceries": ["BigBasket","DMart","Reliance Fresh","Spencer's"],
    "Restaurants": ["Swiggy","Zomato","Barbeque Nation","Biryani Blues","The Fatty Bao"],
    "Travel": ["IRCTC","MakeMyTrip","RedBus","OYO","Radisson","GoAir"],
    "Entertainment": ["BookMyShow","Netflix","Prime Video","Steam","Xbox Store"],
    "Luxury": ["Tanishq","Rolex Shop","Gucci Store","Louis Vuitton","TAG Heuer"],
    "Healthcare": ["Apollo Pharmacy","1mg","Fortis","Max Healthcare"],
    "Utilities": ["BSES","Airtel Payments","BSNL","Reliance Jio"],
    "Education": ["Udemy","Coursera","BYJU'S","Local University"],
    "Online Services": ["Dropbox","Google Cloud","Canva","Slack","Github"]
}
MERCHANT_POOL = [(cat, m) for cat, merchants in CATEGORIES.items() for m in merchants]

# ---- Merchant & payment risk tables (0..1) ----
MERCHANT_RISK = {"Groceries":0.1,"Restaurants":0.2,"Travel":0.4,"Entertainment":0.15,"Luxury":0.9,
                 "Healthcare":0.2,"Utilities":0.05,"Education":0.1,"Online Services":0.3}
PAYMENT_METHOD_RISK = {"Familiar Card":0.2,"Saved Wallet":0.3,"New Wallet":1.0,
                       "NetBanking":0.25,"UPI":0.4,"COD":0.6,"EMI":0.5}
PAYMENT_METHODS = list(PAYMENT_METHOD_RISK.keys())

# ---- Per-user baselines (personalization) ----
NUM_USERS = 120
users = {}
random.seed(42)
for i in range(NUM_USERS):
    uid = f"U{1000+i}"
    avg = max(100, int(random.gauss(2000,1200)))
    std = max(10, abs(int(random.gauss(avg*0.25, avg*0.1))))
    base_txn_day = max(1, int(abs(random.gauss(3,2))))
    fraud_count = 0 if random.random() > 0.03 else random.randint(1,4)
    users[uid] = {
        "avg": float(avg), "std": float(std),
        "base_txn_day": base_txn_day, "fraud_count": fraud_count,
        "recent_ts": deque(), "last_loc": None, "last_time": None
    }

# ---- Scoring constants (your spec) ----
alpha = 0.2                    # scales fraud history
speed_impossible_kmh = 1000    # considered impossible travel
speed_gradient_threshold = 500

# ---- Component functions (exact math you specified) ----
def s_amount(A, user):
    mu, sigma = user["avg"], max(1.0, user["std"])
    val = min(1.0, abs(A - mu) / (3.0 * sigma))
    return 2.0 * val           # s1 weight 2.0

def s_frequency(user, now):
    window = timedelta(minutes=15)
    q = user["recent_ts"]
    while q and (now - q[0]) > window:
        q.popleft()
    N15 = len(q)
    Nbaseline = user["base_txn_day"] / 96.0
    val = min(1.0, N15 / (Nbaseline + 1.0))
    return 1.5 * val         # s2 weight 1.5

def s_fraud_history(user):
    H = user.get("fraud_count", 0)
    val = min(1.0, alpha * H)
    return 2.0 * val         # s3 weight 2.0

def s_location_time(user, new_loc, new_time):
    last_loc, last_time = user["last_loc"], user["last_time"]
    if last_loc is None or last_time is None:
        return 0.0, 0.0
    if last_loc not in CITY_COORDS or new_loc not in CITY_COORDS:
        return 0.0, 0.0
    lat1, lon1 = CITY_COORDS[last_loc]; lat2, lon2 = CITY_COORDS[new_loc]
    km = haversine_km(lat1, lon1, lat2, lon2)
    dt_h = max((new_time - last_time).total_seconds() / 3600.0, 1e-6)
    speed = km / dt_h
    if speed >= speed_impossible_kmh:
        return 2.0, speed
    return 2.0 * min(1.0, speed / speed_gradient_threshold), speed

def s_behavior_distance(user, amount, txn_per_day):
    mu, sigma = user["avg"], max(1.0, user["std"])
    z_amt = abs(amount - mu) / sigma
    z_txn = abs(txn_per_day - user["base_txn_day"]) / max(1.0, user["base_txn_day"])
    D = min(1.0, (z_amt + z_txn) / 6.0)
    return 1.0 * D          # s5 weight 1.0

def merchant_risk(cat): return float(MERCHANT_RISK.get(cat, 0.5))
def payment_risk(method): return float(PAYMENT_METHOD_RISK.get(method, 0.5))

# ---- Transaction generator ----
def generate_transaction():
    uids = list(users.keys())
    weights = [users[u]["base_txn_day"] for u in uids]
    user = random.choices(uids, weights=weights, k=1)[0]
    u = users[user]

    cat, merchant = random.choice(MERCHANT_POOL)
    method = random.choice(PAYMENT_METHODS)

    amount = round(max(10.0, random.gauss(u["avg"], max(1.0, u["std"]))), 2)
    txn_per_day = max(1, int(random.gauss(u["base_txn_day"], max(1, u["base_txn_day"]/2))))
    location = random.choice(list(CITY_COORDS.keys()))
    now = datetime.now(timezone.utc)
    u["recent_ts"].append(now)

    s1 = s_amount(amount, u)
    s2 = s_frequency(u, now)
    s3 = s_fraud_history(u)
    s4, implied_speed = s_location_time(u, location, now)
    s5 = s_behavior_distance(u, amount, txn_per_day)
    s6 = 1.0 * merchant_risk(cat)
    s7 = 0.5 * payment_risk(method)

    injected = None
    if random.random() < ANOMALY_INJECTION_RATE:
        injected = random.choice(["large_amount", "location_jump", "fast_velocity", "payment_method_new", "combo"])
        if injected == "large_amount":
            amount = round(amount * random.uniform(8, 20), 2)
            s1 = s_amount(amount, u)
        elif injected == "location_jump":
            location = random.choice(["London","New York","Paris","Singapore"])
            s4, implied_speed = s_location_time(u, location, now)
        elif injected == "fast_velocity":
            u["last_time"] = now - timedelta(minutes=1)
            u["last_loc"] = random.choice(["London","New York","Paris"])
            s4, implied_speed = s_location_time(u, location, now)
        elif injected == "payment_method_new":
            method = "New Wallet"
            s7 = 0.5 * payment_risk(method)
        elif injected == "combo":
            amount = round(amount * random.uniform(5, 12), 2)
            method = "New Wallet"
            u["last_time"] = now - timedelta(minutes=0.5)
            u["last_loc"] = random.choice(["London","New York"])
            s1 = s_amount(amount, u); s4, implied_speed = s_location_time(u, location, now); s7 = 0.5*payment_risk(method)

    s5 = s_behavior_distance(u, amount, txn_per_day)
    score = round((s1 + s2 + s3 + s4 + s5 + s6 + s7), 3)
    is_anomaly_label = True if score >= 7.0 else False

    u["last_loc"], u["last_time"] = location, now

    txn = {
        "transaction_id": f"TXN{int(time.time()*1000)}{random.randint(1,999)}",
        "user_id": user,
        "timestamp": now.isoformat(),
        "amount": float(amount),
        "txn_per_day": int(txn_per_day),
        "avg_daily_amount": float(u["avg"]),
        "std_amount": float(u["std"]),
        "fraud_history": int(u["fraud_count"]),
        "location": location,
        "merchant_category": cat,
        "merchant": merchant,
        "payment_method": method,
        "implied_speed_kmh": round(implied_speed,1),
        "score_components": {
            "amount_deviation": round(s1,3), "frequency": round(s2,3), "fraud_history": round(s3,3),
            "location_time": round(s4,3), "behavior_fit": round(s5,3),
            "merchant_risk": round(s6,3), "payment_risk": round(s7,3)
        },
        "user_score": float(score),
        "is_anomaly_label": bool(is_anomaly_label),
        "injected_anomaly": injected
    }
    return txn

# ---- Producer loop & controls ----
producing = False
producer_thread = None

def _producer_loop(rate_seconds=RATE_SECONDS):
    global producing
    print("🚀 ----------------- Producer loop started -----------------")
    try:
        while producing:
            txn = generate_transaction()
            producer.send(TOPIC, value=txn)
            icon = "🟢" if not txn["is_anomaly_label"] else "🔴"
            # a neat, spaced single-line log
            print(f"{icon} {datetime.now().strftime('%H:%M:%S')} | Sent {txn['transaction_id']} | user={txn['user_id']} | cat={txn['merchant_category']}({txn['merchant']}) | amt={txn['amount']:.2f} | score={txn['user_score']:.3f} | injected={txn['injected_anomaly']}")
            time.sleep(rate_seconds)
    except Exception as e:
        print("⚠️ Producer error:", e)
    print("🛑 ----------------- Producer loop ended -----------------")

def START_STREAM(rate_seconds=RATE_SECONDS):
    global producing, producer_thread
    if producing:
        print("Producer already running.")
        return
    producing = True
    producer_thread = threading.Thread(target=_producer_loop, args=(rate_seconds,), daemon=True)
    producer_thread.start()
    print("✅ START_STREAM called.")

def STOP_STREAM():
    global producing
    if not producing:
        print("Producer not running.")
        return
    producing = False
    print("⏹️ STOP_STREAM requested; producer will stop shortly.")

def STOP_ALL():
    STOP_STREAM()
    try:
        from consumer import STOP_DETECT as _stopd
        _stopd()
    except Exception:
        pass
    print("🛑 STOP_ALL requested (producer asked to stop).")

print("Producer ready. Call START_STREAM() to begin, STOP_STREAM() to end.")
