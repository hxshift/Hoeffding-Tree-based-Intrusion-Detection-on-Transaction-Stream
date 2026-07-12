# interface.py  (Clean, Organized, Professional)
import tkinter as tk
from tkinter import scrolledtext, ttk
import threading
import queue
import sys
import producer
import consumer
import os
import time
from datetime import datetime

# -----------------------------
# Queues for thread-safe UI updates
# -----------------------------
PRODUCER_Q = queue.Queue()
CONSUMER_Q = queue.Queue()
ALERT_Q = queue.Queue()

# -----------------------------
# History storage for lookup
# -----------------------------
USER_HISTORY = {}   # user_id -> list of transactions

def _history_callback(txn: dict):
    uid = txn.get("user_id")
    if uid:
        USER_HISTORY.setdefault(uid, []).append(txn)

# Attach to consumer module so consumer can call it (consumer code already checks consumer._history_callback)
consumer._history_callback = _history_callback

# -----------------------------
# Router to capture stdout prints (from producer/consumer)
# It will push textual messages into queues; printing modules continue using print()
# -----------------------------
class StdoutRouter:
    def write(self, msg):
        if not msg or msg.strip() == "":
            return
        text = msg.rstrip("\n")
        # classify: producer prints "Sent ...", consumer prints patterns with acc/prec/rec or emoji "🚨"/"✅"
        if "Sent" in text or text.startswith("🚀") or text.startswith("🟢") or text.startswith("🔴"):
            PRODUCER_Q.put(text)
        elif "ANOMALY" in text or "🚨" in text or "⚠️" in text:
            # send to both consumer and alert to ensure visibility
            CONSUMER_Q.put(text)
            ALERT_Q.put(text)
        else:
            # default to consumer queue
            CONSUMER_Q.put(text)
    def flush(self):
        pass

# We'll set sys.stdout to this router when launching UI so subsequent prints from producer/consumer are captured
# (We do this inside launch_interface)

# -----------------------------
# UI implementation 
# -----------------------------
def launch_interface():
    # Replace stdout with router so imported producer/consumer prints get routed
    sys.stdout = StdoutRouter()

    ui = tk.Tk()
    ui.title("Real-Time Fraud Detection Dashboard ")
    ui.geometry("1400x820")
    ui.configure(bg="#101010")

    # Top title
    title = tk.Label(ui, text="Real-Time Transaction Stream & Fraud Detection",
                     bg="#101010", fg="white", font=("Segoe UI", 18, "bold"))
    title.pack(pady=8)

    # Top controls frame
    top_frame = tk.Frame(ui, bg="#101010")
    top_frame.pack(fill="x", padx=10)

    # Threshold slider (visual only but used to highlight rows)
    threshold_var = tk.DoubleVar(value=7.0)
    tk.Label(top_frame, text="Score Threshold Visualiser", bg="#101010", fg="white").pack(side="left", padx=(6,4))
    slider = tk.Scale(top_frame, from_=0, to=10, resolution=0.1, orient="horizontal",
                      variable=threshold_var, length=320, bg="#101010", fg="white", troughcolor="#333")
    slider.pack(side="left")

    # Buttons
    btn_frame = tk.Frame(top_frame, bg="#101010")
    btn_frame.pack(side="right", padx=6)
    def _btn(text, cmd, color="#333"):
        return tk.Button(btn_frame, text=text, command=cmd, width=14, height=1, bg=color, fg="white", font=("Segoe UI",10,"bold"))
    _btn("START STREAM", lambda: threading.Thread(target=producer.START_STREAM, args=(producer.RATE_SECONDS,), daemon=True).start(), color="#2d7a2d").grid(row=0,column=0,padx=4)
    _btn("STOP STREAM", producer.STOP_STREAM, color="#444").grid(row=0,column=1,padx=4)
    _btn("START DETECT", lambda: threading.Thread(target=consumer.START_DETECT, daemon=True).start(), color="#1f6aa5").grid(row=0,column=2,padx=4)
    _btn("STOP DETECT", consumer.STOP_DETECT, color="#444").grid(row=0,column=3,padx=4)
    _btn("STOP ALL", lambda: [producer.STOP_STREAM(), consumer.STOP_DETECT()], color="#8b2f2f").grid(row=0,column=4,padx=4)

    # Main split frame
    main = tk.Frame(ui, bg="#101010")
    main.pack(fill="both", expand=True, padx=10, pady=6)

    # Left: Producer log
    left = tk.Frame(main, bg="#0b0b0b")
    left.pack(side="left", fill="both", expand=True, padx=(0,6))

    tk.Label(left, text="Producer Stream", bg="#0b0b0b", fg="#8ef08e", font=("Segoe UI",14,"bold")).pack(anchor="w", pady=4, padx=6)

    PRODUCER_BOX = scrolledtext.ScrolledText(left, bg="#000000", fg="#8ef08e", font=("Consolas",10), wrap="word")
    PRODUCER_BOX.pack(fill="both", expand=True, padx=6, pady=(0,6))
    PRODUCER_BOX.configure(state="disabled")

    # Right: Consumer log
    right = tk.Frame(main, bg="#0b0b0b")
    right.pack(side="left", fill="both", expand=True, padx=(6,0))

    tk.Label(right, text="Consumer Detection", bg="#0b0b0b", fg="#7ed4f0", font=("Segoe UI",14,"bold")).pack(anchor="w", pady=4, padx=6)

    CONSUMER_BOX = scrolledtext.ScrolledText(right, bg="#000000", fg="#7ed4f0", font=("Consolas",10), wrap="word")
    CONSUMER_BOX.pack(fill="both", expand=True, padx=6, pady=(0,6))
    CONSUMER_BOX.configure(state="disabled")

    # Alerts bottom
    alert_label = tk.Label(ui, text="⚠️ Anomaly Alerts (predicted/labeled)", bg="#101010", fg="#ff9b9b", font=("Segoe UI",12,"bold"))
    alert_label.pack(anchor="w", padx=12)
    ALERT_BOX = scrolledtext.ScrolledText(ui, height=6, bg="#1a0b0b", fg="#ff9b9b", font=("Consolas",10))
    ALERT_BOX.pack(fill="x", padx=12, pady=(0,10))
    ALERT_BOX.configure(state="disabled")

    # --------- Autoscroll toggles: when user scrolls manually, pause autoscroll -----------
    producer_autoscroll = {"on": True}
    consumer_autoscroll = {"on": True}
    def _on_producer_scroll(event=None):
        # if user moves scrollbar not at bottom -> disable autoscroll
        last = PRODUCER_BOX.yview()
        producer_autoscroll["on"] = (last[1] >= 0.999)
    def _on_consumer_scroll(event=None):
        last = CONSUMER_BOX.yview()
        consumer_autoscroll["on"] = (last[1] >= 0.999)
    PRODUCER_BOX.bind("<Button-1>", lambda e: producer_autoscroll.update(on=False))
    PRODUCER_BOX.bind("<MouseWheel>", lambda e: _on_producer_scroll())
    CONSUMER_BOX.bind("<Button-1>", lambda e: consumer_autoscroll.update(on=False))
    CONSUMER_BOX.bind("<MouseWheel>", lambda e: _on_consumer_scroll())

    # --------- Helper to append safely (called from UI thread) -------------
    def append_producer(text):
        PRODUCER_BOX.configure(state="normal")
        PRODUCER_BOX.insert(tk.END, text + "\n\n")   # blank line between txns for clarity
        if producer_autoscroll["on"]:
            PRODUCER_BOX.see(tk.END)
        PRODUCER_BOX.configure(state="disabled")

    def append_consumer(text, highlight=False):
        CONSUMER_BOX.configure(state="normal")
        if highlight:
            # insert with marker line to visually stand out
            CONSUMER_BOX.insert(tk.END, text + "\n", ("highlight",))
            CONSUMER_BOX.tag_configure("highlight", background="#2b2b2b")
        else:
            CONSUMER_BOX.insert(tk.END, text + "\n")
        if consumer_autoscroll["on"]:
            CONSUMER_BOX.see(tk.END)
        CONSUMER_BOX.configure(state="disabled")

    def append_alert(text):
        ALERT_BOX.configure(state="normal")
        ALERT_BOX.insert(tk.END, f"{datetime.now().strftime('%H:%M:%S')} | {text}\n")
        ALERT_BOX.see(tk.END)
        ALERT_BOX.configure(state="disabled")

    # ---------- Polling function: move queue -> widgets (must run on main/UI thread) ----------
    def poll_queues():
        # producer queue
        while not PRODUCER_Q.empty():
            try:
                txt = PRODUCER_Q.get_nowait()
            except queue.Empty:
                break
            append_producer(txt)
        # consumer queue
        while not CONSUMER_Q.empty():
            try:
                txt = CONSUMER_Q.get_nowait()
            except queue.Empty:
                break
            # check threshold to highlight (parse score if present)
            highlight = False
            try:
                if "score=" in txt:
                    # parse like "... score=7.345 ..."
                    seg = txt.split("score=")[1]
                    s = seg.split()[0].strip().strip("|,")
                    score_val = float(s)
                    if score_val >= threshold_var.get():
                        highlight = True
                # else keep default
            except Exception:
                highlight = False
            append_consumer(txt, highlight=highlight)

        # alerts
        while not ALERT_Q.empty():
            try:
                txt = ALERT_Q.get_nowait()
            except queue.Empty:
                break
            append_alert(txt)

        ui.after(200, poll_queues)  # continue polling

    # ---------- User History Lookup button (top-right) ----------
    def open_history_window():
        win = tk.Toplevel(ui)
        win.title("User Transaction History")
        win.geometry("900x600")
        win.configure(bg="#101010")
        tk.Label(win, text="Enter user id (e.g. U1003):", bg="#101010", fg="white").pack(pady=6)
        ent = tk.Entry(win, font=("Segoe UI",12))
        ent.pack(pady=6)

        res = scrolledtext.ScrolledText(win, bg="#000000", fg="white", font=("Consolas",10))
        res.pack(fill="both", expand=True, padx=8, pady=6)
        res.configure(state="disabled")

        def do_search():
            uid = ent.get().strip()
            res.configure(state="normal")
            res.delete("1.0", tk.END)
            if uid in USER_HISTORY and USER_HISTORY[uid]:
                for t in USER_HISTORY[uid]:
                    res.insert(tk.END, f"{t.get('timestamp')} | {t.get('transaction_id')} | amt={t.get('amount'):.2f} | score={t.get('user_score'):.3f} | cat={t.get('merchant_category')}\n")
            else:
                res.insert(tk.END, "No history found for user: " + uid)
            res.configure(state="disabled")

        tk.Button(win, text="Search", command=do_search, bg="#444", fg="white").pack(pady=6)

    tk.Button(top_frame, text="User History Lookup", command=open_history_window, bg="#444", fg="white").pack(side="left", padx=6)

    # Start polling
    ui.after(200, poll_queues)

    # When UI closes, clean up: ask producer/consumer to stop
    def on_close():
        try:
            producer.STOP_STREAM()
        except:
            pass
        try:
            consumer.STOP_DETECT()
        except:
            pass
        ui.destroy()
    ui.protocol("WM_DELETE_WINDOW", on_close)

    # Bring window to front
    ui.lift()
    ui.attributes("-topmost", True)
    ui.after_idle(ui.attributes, '-topmost', False)

    ui.mainloop()
