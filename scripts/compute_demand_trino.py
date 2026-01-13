import os
import json
import subprocess
import psycopg2
from datetime import datetime
from trino.dbapi import connect

# --- CONFIGURATION ---
def get_trino_host():
    # If running inside Docker, use the container name 'trino'
    if os.path.exists('/.dockerenv'): return "trino"
    return "localhost"

TRINO_HOST = get_trino_host()
TRINO_PORT = 8080
TRINO_USER = "admin"

# Update DB Params to use the same logic
def get_db_host():
    if os.path.exists('/.dockerenv'): return "postgres"
    return "localhost"

DB_PARAMS = {
    "host": get_db_host(),
    "port": "5432",
    "database": "procurement_db",
    "user": "user",
    "password": "password"
}

# CHANGE DATE HERE IF NEEDED (Default: Today)
DATE_TO_PROCESS = datetime.now().strftime("%Y-%m-%d")
LOCAL_OUTPUT_DIR = "./generated_data/supplier_orders_trino"

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def get_master_data(conn):
    print("🐘 Fetching Master Data from PostgreSQL...")
    with conn.cursor() as cur:
        query = """
        SELECT p.sku, p.name, p.supplier_id, s.name, r.safety_stock, r.moq 
        FROM products p 
        JOIN suppliers s ON p.supplier_id = s.supplier_id 
        JOIN replenishment_rules r ON p.sku = r.sku
        """
        cur.execute(query)
        data = {row[0]: {"name": row[1], "sup_id": row[2], "sup_name": row[3], "safety": row[4], "moq": row[5]} for row in cur.fetchall()}
    return data

def run_trino_aggregation(date_str):
    print(f"🚀 Sending Aggregation Query to Trino for {date_str}...")
    
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="hive", schema="default")
    cur = conn.cursor()
    
    # 1. Sync Partitions
    cur.execute("CALL system.sync_partition_metadata('default', 'raw_orders', 'FULL')")
    cur.execute("CALL system.sync_partition_metadata('default', 'raw_inventory', 'FULL')")
    
    # 2. Aggregation Query
    query = f"""
    SELECT 
        COALESCE(o.sku, i.sku) as sku,
        COALESCE(o.total_sold, 0) as total_sold,
        COALESCE(i.total_avail, 0) as total_avail,
        COALESCE(i.total_reserved, 0) as total_reserved
    FROM (
        SELECT t.sku, SUM(t.quantity) as total_sold
        FROM raw_orders 
        CROSS JOIN UNNEST(items) AS t(sku, quantity, unit_price)
        WHERE dt = '{date_str}'
        GROUP BY t.sku
    ) o
    FULL OUTER JOIN (
        SELECT sku, SUM(CAST(available_qty AS INT)) as total_avail, SUM(CAST(reserved_qty AS INT)) as total_reserved
        FROM raw_inventory
        WHERE dt = '{date_str}'
        GROUP BY sku
    ) i ON o.sku = i.sku
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    return rows

def generate_supplier_files(trino_results, master_data, date_str):
    print("🧠 Calculating Net Demand...")
    if os.path.exists(LOCAL_OUTPUT_DIR):
        import shutil
        shutil.rmtree(LOCAL_OUTPUT_DIR)
    os.makedirs(LOCAL_OUTPUT_DIR)
    
    supplier_batches = {} 
    
    for row in trino_results:
        sku, sold, avail, reserved = row
        if sku not in master_data: continue
            
        info = master_data[sku]
        net_demand = max(0, sold + info['safety'] - (avail - reserved))
        
        if net_demand > 0:
            qty_to_order = max(net_demand, info['moq'])
            sup_name = info['sup_name']
            if sup_name not in supplier_batches: supplier_batches[sup_name] = []
            
            supplier_batches[sup_name].append({
                "sku": sku,
                "product": info['name'],
                "net_demand": net_demand,
                "final_order_quantity": qty_to_order
            })
            
    for sup, items in supplier_batches.items():
        filename = f"Order_{sup.replace(' ', '_')}_{date_str}.json"
        with open(f"{LOCAL_OUTPUT_DIR}/{filename}", "w") as f:
            json.dump({"supplier": sup, "date": date_str, "origin": "Computed via Trino", "items": items}, f, indent=2)
            
    return LOCAL_OUTPUT_DIR

# --- UPDATED UPLOAD FUNCTION ---
def upload_to_hdfs(local_dir, date_str):
    # Target path: /output/supplier_orders/2026-01-08
    parent_dir = "/output/supplier_orders"
    final_target = f"{parent_dir}/{date_str}"
    
    print(f"📂 Uploading results to HDFS folder: {final_target} ...")
    
    # 1. Clean existing folder for this date (Prevents duplicates/errors)
    subprocess.run(f"docker exec namenode hdfs dfs -rm -r -f {final_target}", shell=True, stderr=subprocess.DEVNULL)
    
    # 2. Ensure parent exists
    subprocess.run(f"docker exec namenode hdfs dfs -mkdir -p {parent_dir}", shell=True)
    
    # 3. Use a TEMP folder inside the container named exactly like the date
    # This ensures that when we 'put' it, it appears as that folder name.
    container_temp = f"/tmp/{date_str}"
    
    # Clean temp inside container
    subprocess.run(f"docker exec namenode rm -rf {container_temp}", shell=True)
    
    # 4. Copy Local Files -> Docker Container Temp Folder
    subprocess.run(f"docker cp \"{local_dir}\" namenode:\"{container_temp}\"", shell=True)
    
    # 5. Move from Container -> HDFS
    # Since container_temp is named "2026-01-08", this puts that folder into supplier_orders
    subprocess.run(f"docker exec namenode hdfs dfs -put \"{container_temp}\" {parent_dir}/", shell=True)
    
    # 6. Cleanup
    subprocess.run(f"docker exec namenode rm -rf {container_temp}", shell=True)
    print("✅ Upload complete.")

if __name__ == "__main__":
    # 1. Get Master Data
    pg_conn = get_db_connection()
    rules = get_master_data(pg_conn)
    pg_conn.close()
    
    # 2. Run Trino Compute
    aggregates = run_trino_aggregation(DATE_TO_PROCESS)
    
    # 3. Generate & Upload
    output_dir = generate_supplier_files(aggregates, rules, DATE_TO_PROCESS)
    
    # Pass the date_str to the upload function now!
    upload_to_hdfs(output_dir, DATE_TO_PROCESS)