from __future__ import annotations
import argparse, hashlib, json, logging, os, smtplib, ssl, sqlite3, time
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Callable
import requests
from dotenv import load_dotenv
From contextlib import contextmanager

load_dotenv()
API_URL = os.getenv("API_URL", "https://dummyjson.com/products?limit=10")
DB_PATH = Path(os.getenv("MONITOR_DB_PATH", "business_monitor.db"))
LOG_PATH = Path(os.getenv("MONITOR_LOG_PATH", "business_monitor.log"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

logger = logging.getLogger("business_monitor")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except OSError:
        logger.warning("Could not create log file: %s", LOG_PATH)

@contextmanager
def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    try:
        with conn:
            yield conn
    finally:
        conn.close()

def initialize_database():
    with get_connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS product_snapshots (
            product_id INTEGER PRIMARY KEY, title TEXT NOT NULL,
            price REAL NOT NULL, stock INTEGER NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS detected_events (
            event_id TEXT PRIMARY KEY, product_id INTEGER NOT NULL,
            product TEXT NOT NULL, event_type TEXT NOT NULL, old_value TEXT,
            new_value TEXT, detected_at TEXT NOT NULL,
            alert_status TEXT NOT NULL DEFAULT 'PENDING'
        );
        """)

def fetch_products():
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("products"), list):
        raise ValueError("Unexpected API response: expected a products list")
    products = {}
    for item in payload["products"]:
        for key in ("id", "title", "price", "stock"):
            if key not in item:
                raise ValueError(f"Product missing required field: {key}")
        pid = int(item["id"])
        products[pid] = {"id": pid, "title": str(item["title"]),
                         "price": float(item["price"]), "stock": int(item["stock"])}
    return products

def fetch_with_retry(fetch_fn: Callable = fetch_products, max_attempts=MAX_ATTEMPTS,
                     base_delay=1, sleep_fn=time.sleep):
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    for attempt in range(1, max_attempts + 1):
        try:
            data = fetch_fn()
            logger.info("API fetch succeeded on attempt %s.", attempt)
            return data
        except requests.RequestException as exc:
            status = exc.response.status_code if isinstance(exc, requests.HTTPError) and exc.response is not None else None
            retryable = status in RETRYABLE_STATUS_CODES if status is not None else not isinstance(exc, requests.HTTPError)
            if not retryable or attempt == max_attempts:
                logger.error("API fetch failed on attempt %s/%s: %s", attempt, max_attempts, exc)
                raise
            delay = base_delay * 2 ** (attempt - 1)
            logger.warning("API failure on attempt %s/%s; retrying in %s seconds: %s",
                           attempt, max_attempts, delay, exc)
            sleep_fn(delay)

def load_snapshot():
    with get_connection() as conn:
        rows = conn.execute("SELECT product_id,title,price,stock FROM product_snapshots").fetchall()
    return {r["product_id"]: {"id": r["product_id"], "title": r["title"],
                              "price": r["price"], "stock": r["stock"]} for r in rows}

def save_snapshot(products):
    now = datetime.now(timezone.utc).isoformat()
    records = [(pid, p["title"], p["price"], p["stock"], now) for pid, p in products.items()]
    with get_connection() as conn:
        conn.executemany("""
        INSERT INTO product_snapshots(product_id,title,price,stock,updated_at)
        VALUES(?,?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET
        title=excluded.title, price=excluded.price, stock=excluded.stock,
        updated_at=excluded.updated_at
        """, records)

def detect_changes(previous, current):
    events = []
    for pid, product in current.items():
        old = previous.get(pid)
        if old is None:
            events.append({"product_id":pid,"product":product["title"],"event":"NEW_PRODUCT",
                           "old_value":None,"new_value":product})
            continue
        if old["price"] != product["price"]:
            events.append({"product_id":pid,"product":product["title"],"event":"PRICE_CHANGED",
                           "old_value":old["price"],"new_value":product["price"]})
        if old["stock"] != product["stock"]:
            events.append({"product_id":pid,"product":product["title"],"event":"STOCK_CHANGED",
                           "old_value":old["stock"],"new_value":product["stock"]})
        if old["stock"] > 0 and product["stock"] == 0:
            events.append({"product_id":pid,"product":product["title"],"event":"OUT_OF_STOCK",
                           "old_value":old["stock"],"new_value":0})
    return events

def create_event_id(event):
    identity = {k:event[k] for k in ("product_id","event","old_value","new_value")}
    raw = json.dumps(identity, sort_keys=True, separators=(",",":"))
    return hashlib.sha256(raw.encode()).hexdigest()

def store_events(events):
    inserted, now = 0, datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        for event in events:
            cur = conn.execute("""
            INSERT INTO detected_events(event_id,product_id,product,event_type,old_value,new_value,detected_at,alert_status)
            VALUES(?,?,?,?,?,?,?,'PENDING') ON CONFLICT(event_id) DO NOTHING
            """, (create_event_id(event), event["product_id"], event["product"], event["event"],
                  json.dumps(event["old_value"],sort_keys=True), json.dumps(event["new_value"],sort_keys=True), now))
            inserted += cur.rowcount
    logger.info("New events stored: %s; duplicates skipped: %s", inserted, len(events)-inserted)
    return inserted

def get_pending_events():
    with get_connection() as conn:
        rows = conn.execute("""SELECT event_id,product_id,product,event_type,old_value,new_value,detected_at
                               FROM detected_events WHERE alert_status='PENDING' ORDER BY detected_at""").fetchall()
    return [dict(r) for r in rows]

def mark_events_sent(ids):
    with get_connection() as conn:
        conn.executemany("UPDATE detected_events SET alert_status='SENT' WHERE event_id=? AND alert_status='PENDING'",
                         [(event_id,) for event_id in ids])

def send_email(subject, body):
    sender = os.getenv("SMTP_EMAIL","").strip()
    password = os.getenv("SMTP_APP_PASSWORD","").replace(" ","")
    recipient = os.getenv("ALERT_RECIPIENT","").strip()
    if not sender or not password or not recipient:
        raise RuntimeError("Email is not configured; set SMTP_EMAIL, SMTP_APP_PASSWORD, and ALERT_RECIPIENT.")
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = sender, recipient, subject
    msg.set_content(body)
    with smtplib.SMTP_SSL("smtp.gmail.com",465,context=ssl.create_default_context(),timeout=30) as server:
        server.login(sender,password)
        server.send_message(msg)
    logger.info("Email accepted by SMTP server for recipient %s", recipient)
    return True

def format_alert_email(events):
    subject = f"Business Data Monitor: {len(events)} Product Change Alert(s)"
    lines = ["BUSINESS DATA MONITOR","Product Change Report","="*45,f"Total changes: {len(events)}",""]
    for i,e in enumerate(events,1):
        lines += [f"{i}. {e['product']}",f"   Event: {e['event_type']}",
                  f"   Previous value: {e['old_value']}",f"   Current value: {e['new_value']}",
                  f"   Detected at: {e['detected_at']}",""]
    lines.append("Generated automatically by Business Data Monitor.")
    return subject, "\n".join(lines)

def process_pending_alerts(sender: Callable = send_email):
    events = get_pending_events()
    if not events:
        logger.info("No pending alerts. Nothing to send.")
        return 0
    subject, body = format_alert_email(events)
    try:
        if sender(subject,body) is not True:
            raise RuntimeError("Email sender did not confirm success")
    except Exception as exc:
        logger.error("Alert submission failed: %s: %s",type(exc).__name__,exc)
        return 0
    mark_events_sent([e["event_id"] for e in events])
    logger.info("Marked %s events as SENT.",len(events))
    return len(events)

def run_monitor(fetcher: Callable = fetch_with_retry, sender: Callable = send_email):
    logger.info("="*50)
    logger.info("Business Data Monitor run started.")
    try:
        current = fetcher()
        if not current:
            raise ValueError("API returned no products; refusing to save an empty snapshot.")
        previous = load_snapshot()
        if not previous:
            save_snapshot(current)
            logger.info("Baseline created with %s products. No alerts generated.",len(current))
            return {"status":"BASELINE_CREATED","products":len(current),"changes":0,
                    "new_events":0,"alerts_sent":0,"pending_alerts":len(get_pending_events())}
        changes = detect_changes(previous,current)
        new_events = store_events(changes)
        logger.info("Products fetched: %s | Changes detected: %s | New events stored: %s",
                    len(current),len(changes),new_events)
        sent = process_pending_alerts(sender=sender)
        pending = len(get_pending_events())
        save_snapshot(current)
        status = "SUCCESS" if pending == 0 else "ALERTS_PENDING"
        logger.info("Monitoring run completed. Status: %s | Alerts sent: %s | Pending: %s",
                    status,sent,pending)
        return {"status":status,"products":len(current),"changes":len(changes),
                "new_events":new_events,"alerts_sent":sent,"pending_alerts":pending}
    except Exception as exc:
        logger.exception("Monitoring run failed: %s",type(exc).__name__)
        return {"status":"ERROR","error":str(exc)}

def main():
    parser = argparse.ArgumentParser(description="Monitor product data and send change alerts.")
    parser.add_argument("--interval",type=float,default=0,help="Repeat every N minutes; 0 runs once.")
    args = parser.parse_args()
    if args.interval < 0:
        parser.error("--interval must be >= 0")
    initialize_database()
    if args.interval == 0:
        run_monitor()
        return
    logger.info("Scheduler started. Interval: %s minutes.",args.interval)
    try:
        while True:
            run_monitor()
            time.sleep(args.interval*60)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")

if __name__ == "__main__":
    main()
