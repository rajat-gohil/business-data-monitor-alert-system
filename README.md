# Business Data Monitor & Alert System

A Python portfolio project that monitors product data from DummyJSON, detects changes, stores snapshots/events in SQLite, retries temporary API failures, logs runs, and sends Gmail SMTP summary alerts.

## Features
- REST API fetching with timeout and exponential-backoff retries
- New-product, price-change, stock-change, and out-of-stock detection
- SQLite snapshots and event history with duplicate-event fingerprints
- Gmail SMTP summary alerts; failed alerts remain pending
- Timestamped console/file logging
- One-off or interval-based runs
- Automated tests using temporary databases and fake email senders

## Setup
Requires Python 3.10+.

```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and configure it locally. Never commit `.env`.

For Gmail alerts, enable 2-Step Verification and create an App Password at https://myaccount.google.com/apppasswords. Use the App Password, not your regular account password.

## Run once
```bash
python monitor.py
```
The first run creates a baseline without emailing all existing products. Later runs compare current data with the saved snapshot.

## Run every 5 minutes
```bash
python monitor.py --interval 5
```
Stop with Ctrl+C. This loop only runs while the process is alive; it is not a managed cloud scheduler.

## Tests
```bash
python -m unittest discover -s tests -v
```
Tests do not call the live API or send real emails.

## Configuration
- `API_URL`: API endpoint (default DummyJSON, first 10 products)
- `MONITOR_DB_PATH`: SQLite database path
- `MONITOR_LOG_PATH`: log file path
- `MAX_ATTEMPTS`: API retry limit
- `SMTP_EMAIL`, `SMTP_APP_PASSWORD`, `ALERT_RECIPIENT`: optional Gmail settings

If email is not configured, events remain pending and the monitor logs the issue.

## Security and limitations
- Never commit `.env`, App Passwords, database files, or logs.
- Revoke and replace any credential that is accidentally exposed.
- SMTP acceptance does not guarantee inbox delivery. A crash after SMTP acceptance but before updating SQLite can cause a repeated email.
- Event fingerprints suppress identical transitions if they recur later; production systems may track each occurrence/run separately.
- SQLite is appropriate for a small demo. Production deployment should use a managed scheduler and hosted database.

## Portfolio summary
**Problem:** Product data can change without someone checking it regularly.

**Solution:** An automated monitor compares snapshots, persists changes, and sends email summaries.

**Skills:** Python, REST APIs, retries, SQLite, SQL upserts, SMTP, logging, automated testing, scheduling, and basic idempotency.
