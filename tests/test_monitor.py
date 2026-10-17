import tempfile
import unittest
from pathlib import Path
import requests
import monitor

def products(price=100.0, stock=10):
    return {1: {"id":1,"title":"Test Mouse","price":price,"stock":stock}}

class MonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.old_path = monitor.DB_PATH
        monitor.DB_PATH = Path(self.temp.name)/"test.db"
        monitor.initialize_database()

    def tearDown(self):
        monitor.DB_PATH = self.old_path
        self.temp.cleanup()

    def test_retry_then_success(self):
        calls, delays = {"n":0}, []
        def fetch():
            calls["n"] += 1
            if calls["n"] < 3:
                raise requests.ConnectionError("temporary")
            return products()
        result = monitor.fetch_with_retry(fetch_fn=fetch, max_attempts=3, sleep_fn=delays.append)
        self.assertIn(1,result)
        self.assertEqual(calls["n"],3)
        self.assertEqual(delays,[1,2])

    def test_price_stock_and_out_of_stock_changes(self):
        events = monitor.detect_changes(products(100,10),products(80,0))
        kinds = {e["event"] for e in events}
        self.assertTrue({"PRICE_CHANGED","STOCK_CHANGED","OUT_OF_STOCK"}.issubset(kinds))

    def test_baseline_then_price_alert(self):
        messages = []
        self.assertEqual(monitor.run_monitor(fetcher=lambda:products(),sender=lambda s,b:True)["status"],"BASELINE_CREATED")
        result = monitor.run_monitor(fetcher=lambda:products(80),sender=lambda s,b:(messages.append((s,b)) or True))
        self.assertEqual(result["status"],"SUCCESS")
        self.assertEqual(result["changes"],1)
        self.assertEqual(result["alerts_sent"],1)
        self.assertEqual(len(messages),1)
        self.assertEqual(monitor.get_pending_events(),[])

    def test_failed_email_remains_pending_and_recovers(self):
        monitor.save_snapshot(products())
        event = {"product_id":1,"product":"Test Mouse","event":"PRICE_CHANGED","old_value":100.0,"new_value":80.0}
        monitor.store_events([event])
        def fail(s,b): raise RuntimeError("simulated SMTP failure")
        self.assertEqual(monitor.process_pending_alerts(sender=fail),0)
        self.assertEqual(len(monitor.get_pending_events()),1)
        self.assertEqual(monitor.process_pending_alerts(sender=lambda s,b:True),1)
        self.assertEqual(monitor.get_pending_events(),[])

    def test_api_failure_preserves_snapshot(self):
        original = products()
        monitor.save_snapshot(original)
        def fail(): raise requests.ConnectionError("simulated outage")
        result = monitor.run_monitor(fetcher=fail,sender=lambda s,b:True)
        self.assertEqual(result["status"],"ERROR")
        self.assertEqual(monitor.load_snapshot(),original)

    def test_duplicate_event_is_skipped(self):
        event = {"product_id":1,"product":"Test Mouse","event":"PRICE_CHANGED","old_value":100.0,"new_value":80.0}
        self.assertEqual(monitor.store_events([event]),1)
        self.assertEqual(monitor.store_events([event]),0)

if __name__ == "__main__":
    unittest.main()
