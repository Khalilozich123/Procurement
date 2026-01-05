import time
import subprocess
import datetime
import os
import sys

# --- CONFIGURATION ---
TARGET_HOUR = 22  # Run at 10 PM
TARGET_MINUTE = 00
CHECK_INTERVAL = 60  # Check time every 60 seconds

# Path to your Python executable (handles venv if you use one)
PYTHON_EXEC = sys.executable 

def log(message):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def run_pipeline():
    log("🚀 STARTING DAILY BATCH PIPELINE...")
    start_timer = time.time()

    try:
        # STEP 1: Generate Data & Upload to HDFS
        log("1️⃣  Running Data Generator (generate_moroccan_data.py)...")
        subprocess.run([PYTHON_EXEC, "generate_orders_bulk.py"], check=True)

        # STEP 2: Sync Partitions in Trino (Crucial!)
        log("2️⃣  Syncing Hive Partitions...")
        sync_query = "CALL hive.system.sync_partition_metadata('default', 'raw_orders', 'ADD'); CALL hive.system.sync_partition_metadata('default', 'raw_inventory', 'ADD');"
        subprocess.run(
            ["docker", "exec", "-i", "trino", "trino", "--execute", sync_query], 
            check=True
        )

        # STEP 3: Run Analytics (Purchase Orders)
        # Note: We create a fresh table for today's date
        log("3️⃣  Calculating Purchase Orders (Business Logic)...")
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # We drop the table if it exists to allow re-runs, or you could append. 
        # For this batch job, a fresh snapshot is safer.
        analytics_query = f"""
        DROP TABLE IF EXISTS hive.default.purchase_orders;
        
        CREATE TABLE hive.default.purchase_orders 
        WITH (
            format = 'ORC',
            external_location = 'hdfs://namenode:9000/processed/purchase_orders_{today_str.replace('-','')}'
        )
        AS
        WITH daily_sales AS (
            SELECT t.sku, SUM(t.quantity) AS total_sold
            FROM hive.default.raw_orders
            CROSS JOIN UNNEST(items) AS t(sku, quantity, unit_price)
            WHERE dt = '{today_str}'
            GROUP BY t.sku
        ),
        daily_inventory AS (
            SELECT sku, SUM(CAST(available_qty AS INT)) AS total_on_hand
            FROM hive.default.raw_inventory
            WHERE dt = '{today_str}'
            GROUP BY sku
        )
        SELECT 
            i.sku,
            i.total_on_hand,
            COALESCE(s.total_sold, 0) AS sold_today,
            (i.total_on_hand - COALESCE(s.total_sold, 0)) AS remaining_stock,
            CASE 
                WHEN (i.total_on_hand - COALESCE(s.total_sold, 0)) < 500 THEN 'YES' 
                ELSE 'NO' 
            END AS restock_needed,
            '{today_str}' AS calculation_date
        FROM daily_inventory i
        LEFT JOIN daily_sales s ON i.sku = s.sku;
        """
        
        subprocess.run(
            ["docker", "exec", "-i", "trino", "trino", "--execute", analytics_query], 
            check=True
        )

        elapsed = round(time.time() - start_timer, 2)
        log(f"✅ BATCH COMPLETE in {elapsed} seconds. Waiting for next schedule...")

    except subprocess.CalledProcessError as e:
        log(f"❌ PIPELINE FAILED: {e}")
    except Exception as e:
        log(f"❌ UNEXPECTED ERROR: {e}")

def start_scheduler():
    log(f"🕒 Scheduler initialized. Waiting for {TARGET_HOUR:02d}:{TARGET_MINUTE:02d}...")
    
    while True:
        now = datetime.datetime.now()
        
        # Check if it matches the target time
        if now.hour == TARGET_HOUR and now.minute == TARGET_MINUTE:
            run_pipeline()
            # Sleep for 61 seconds so we don't trigger it twice in the same minute
            time.sleep(61)
        else:
            # Optional: Heartbeat log every hour so you know it's alive
            if now.minute == 0 and now.second < 5:
                log("💤 Scheduler is alive and waiting...")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    # OPTION: Uncomment the line below to run immediately once for testing
    # run_pipeline()
    
    start_scheduler()