import psycopg2
import json
import csv
import random
import os
import subprocess
import shutil
import sys
from datetime import datetime
from faker import Faker

# --- CONFIGURATION ---
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

ORDERS_PER_DAY = 5000 
DATE_TO_GENERATE = datetime.now().strftime("%Y-%m-%d")
LOCAL_OUTPUT_DIR = "./generated_data" if os.name == 'nt' else "/app/generated_data"
fake = Faker()

# --- MASTER DATA ---
SUPPLIERS = [
    ("SUP-001", "Les Eaux Minérales d'Oulmès", "Casablanca"),
    ("SUP-002", "Centrale Danone", "Casablanca"),
    ("SUP-003", "Dari Couspate", "Salé"),
    ("SUP-004", "Cosumar", "Casablanca"),
    ("SUP-005", "Dislog Group", "Casablanca")
]

MOROCCAN_PRODUCTS = [
    # SKU, Name, Price, SupplierID, SafetyStock, MOQ
    ("PRD-001", "Sidi Ali 1.5L", 6.50, "SUP-001", 100, 50),
    ("PRD-002", "Oulmes 1L", 7.00, "SUP-001", 80, 40),
    ("PRD-003", "Couscous Dari 1kg", 13.50, "SUP-003", 50, 20),
    ("PRD-004", "Thé Sultan Vert", 18.00, "SUP-004", 60, 20),
    ("PRD-005", "Aicha Confiture Fraise", 22.00, "SUP-005", 30, 10),
    ("PRD-006", "Lait Centrale Danone", 3.50, "SUP-002", 200, 100),
    ("PRD-007", "Raibi Jamila", 2.50, "SUP-002", 250, 100),
    ("PRD-008", "Huile d'Olive Al Horra", 65.00, "SUP-005", 20, 10),
    ("PRD-009", "Fromage La Vache Qui Rit", 15.00, "SUP-005", 40, 20),
    ("PRD-010", "Merendina", 2.00, "SUP-005", 300, 50),
    ("PRD-011", "Pasta Tria", 8.00, "SUP-003", 60, 20),
    ("PRD-012", "Sardines Titus", 5.50, "SUP-005", 80, 20),
    ("PRD-013", "Coca-Cola 1L", 9.00, "SUP-005", 150, 30),
    ("PRD-014", "Atay Sebou", 14.00, "SUP-004", 50, 10),
    ("PRD-015", "Eau Ciel 5L", 12.00, "SUP-005", 40, 10)
]

STORES = [
    ("STORE-CAS-01", "Marjane Californie"), 
    ("STORE-CAS-02", "Morocco Mall"), 
    ("STORE-RAB-01", "Marjane Hay Riad"), 
    ("STORE-TNG-01", "Socco Alto"), 
    ("STORE-MAR-01", "Menara Mall"), 
    ("STORE-AGA-01", "Carrefour Agadir"), 
    ("STORE-FES-01", "Borj Fez")
]

def get_db_connection():
    try: return psycopg2.connect(**DB_PARAMS)
    except:
        print("❌ DB Connection Error. Is Docker running?")
        sys.exit(1)

def seed_database(conn):
    print("🇲🇦 Seeding Database...")
    with conn.cursor() as cur:
        for t in ["replenishment_rules", "products", "suppliers", "warehouses", "stores"]: 
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
        
        cur.execute("CREATE TABLE suppliers (supplier_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100), city VARCHAR(50))")
        for s in SUPPLIERS: cur.execute("INSERT INTO suppliers VALUES (%s, %s, %s)", s)
        
        cur.execute("CREATE TABLE products (sku VARCHAR(50) PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), supplier_id VARCHAR(50))")
        for p in MOROCCAN_PRODUCTS: cur.execute("INSERT INTO products VALUES (%s, %s, %s, %s)", (p[0], p[1], p[2], p[3]))
        
        cur.execute("CREATE TABLE replenishment_rules (sku VARCHAR(50) PRIMARY KEY, safety_stock INT, moq INT)")
        for p in MOROCCAN_PRODUCTS: cur.execute("INSERT INTO replenishment_rules VALUES (%s, %s, %s)", (p[0], p[4], p[5]))
        
        cur.execute("CREATE TABLE warehouses (warehouse_id VARCHAR(50) PRIMARY KEY, store_id VARCHAR(50))")
        # Creating STORES table explicitly as requested before
        cur.execute("CREATE TABLE stores (store_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100))")
        
        for s in STORES: 
            cur.execute("INSERT INTO warehouses VALUES (%s, %s)", (f"WH-{s[0]}", s[0]))
            cur.execute("INSERT INTO stores VALUES (%s, %s)", (s[0], s[1]))
            
        conn.commit()

def fetch_master_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT p.sku, p.name, p.price FROM products p")
        products = {row[0]: {"name": row[1], "price": float(row[2])} for row in cur.fetchall()}
        
        cur.execute("SELECT store_id FROM warehouses")
        stores = [row[0].replace("WH-", "") for row in cur.fetchall()]
    return products, stores

def generate_and_process(products, stores, date_str):
    print(f"🚀 Generating Raw Data (Orders & Inventory) for {date_str}...")
    if os.path.exists(LOCAL_OUTPUT_DIR):
        for filename in os.listdir(LOCAL_OUTPUT_DIR):
            file_path = os.path.join(LOCAL_OUTPUT_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"⚠️ Failed to delete {file_path}. Reason: {e}")
    else:
        os.makedirs(LOCAL_OUTPUT_DIR)
    
    # 1. Initialize Exception Tracking
    exceptions = []
    
    # --- 2. Generate Orders (JSON) ---
    base_path_orders = f"{LOCAL_OUTPUT_DIR}/orders/dt={date_str}"
    sales_counts = {sku: 0 for sku in products} 
    files = {}
    active_stores = set()

    for sid in stores:
        p = f"{base_path_orders}/store_id={sid}"
        os.makedirs(p, exist_ok=True)
        files[sid] = open(f"{p}/orders.json", "w", encoding="utf-8")

    skus = list(products.keys())
    for i in range(ORDERS_PER_DAY):
        store = random.choice(stores)
        active_stores.add(store)
        items = []
        for _ in range(random.randint(1, 3)):
            sku = random.choice(skus)
            qty = random.randint(1, 5)
            
            # EXCEPTION CHECK: Spike detection
            if qty > 4: 
                exceptions.append(f"WARNING: Demand Spike detected. Order {i} for {sku} has qty {qty}.")
                
            sales_counts[sku] += qty
            items.append({"sku": sku, "quantity": qty, "unit_price": products[sku]['price']})
        
        order = {"order_id": fake.uuid4(), "timestamp": f"{date_str}T{fake.time()}", "items": items}
        files[store].write(json.dumps(order) + '\n')
    
    for f in files.values(): f.close()

    # EXCEPTION CHECK: Did any store fail to report?
    for sid in stores:
        if sid not in active_stores:
            exceptions.append(f"CRITICAL: Missing POS files for {sid}. No orders received.")

    # --- 3. Generate Inventory (CSV) ---
    base_path_inv = f"{LOCAL_OUTPUT_DIR}/inventory/dt={date_str}"
    os.makedirs(base_path_inv, exist_ok=True)
    
    with open(f"{base_path_inv}/inventory.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["warehouse_id", "sku", "available_qty", "reserved_qty"])
        for store in stores:
            wh_id = f"WH-{store}"
            for sku in skus:
                store_sales_share = sales_counts[sku] // len(stores)
                
                if random.random() > 0.5:
                    start_stock = store_sales_share + random.randint(10, 50)
                else:
                    start_stock = max(0, store_sales_share - random.randint(0, 5))
                
                reserved = random.randint(0, 2)
                writer.writerow([wh_id, sku, start_stock, reserved])

    # --- 4. Exception Report Only ---
    # (Removed Compute Net Demand Logic)
    
    base_path_logs = f"{LOCAL_OUTPUT_DIR}/logs/exceptions"
    os.makedirs(base_path_logs, exist_ok=True)
    report_file = f"{base_path_logs}/report_{date_str}.txt"
    
    with open(report_file, "w") as f:
        f.write(f"--- GENERATION REPORT: {date_str} ---\n")
        f.write(f"Status: {'SUCCESS' if not exceptions else 'WARNING'}\n")
        f.write(f"Processed Orders: {ORDERS_PER_DAY}\n")
        if exceptions:
            f.write("DETECTED ANOMALIES:\n")
            for e in exceptions:
                f.write(f"[!] {e}\n")
        else:
            f.write("No data generation anomalies detected.\n")

    return base_path_orders, base_path_inv, base_path_logs

def upload_to_hdfs(local_dir, hdfs_target_parent):
    # Determine the folder name (e.g., "dt=2026-01-05" or "exceptions")
    folder_name = os.path.basename(local_dir)
    
    print(f"📂 Uploading {folder_name} -> {hdfs_target_parent}...")
    
    # 1. Define Paths
    if "logs" in local_dir:
         # Logs go directly into the parent folder
         full_hdfs_path = f"{hdfs_target_parent}"
    else:
         # Orders/Inventory go into a subfolder (/raw/orders/dt=2026-01-05)
         full_hdfs_path = f"{hdfs_target_parent}/{folder_name}"

    # 2. Clean previous data in HDFS
    if "logs" not in local_dir:
        subprocess.run(f"docker exec namenode hdfs dfs -rm -r -f {full_hdfs_path}", shell=True, stderr=subprocess.DEVNULL)
    
    # 3. Ensure Target Directory Exists
    subprocess.run(f"docker exec namenode hdfs dfs -mkdir -p {hdfs_target_parent}", shell=True, check=True)
    
    # 4. Copy to Docker Container Temp
    container_temp = f"/tmp/{folder_name}"
    subprocess.run(f"docker exec namenode rm -rf {container_temp}", shell=True)
    subprocess.run(f"docker cp \"{local_dir}\" namenode:\"{container_temp}\"", shell=True, check=True)
    
    # 5. Put to HDFS
    if "logs" in local_dir:
         subprocess.run(f"docker exec namenode hdfs dfs -put -f \"{container_temp}/.\" {hdfs_target_parent}/", shell=True, check=True)
    else:
         subprocess.run(f"docker exec namenode hdfs dfs -put \"{container_temp}\" {hdfs_target_parent}/", shell=True, check=True)
    
    # 6. Cleanup
    subprocess.run(f"docker exec namenode rm -rf {container_temp}", shell=True)
    print("✅ Upload complete.")

if __name__ == "__main__":
    conn = get_db_connection()
    seed_database(conn)
    prods, stores = fetch_master_data(conn)
    conn.close()
    
    # Run Generation Cycle
    o_path, i_path, log_path = generate_and_process(prods, stores, DATE_TO_GENERATE)
    
    # Upload Raw Data to HDFS
    upload_to_hdfs(o_path, "/raw/orders")
    upload_to_hdfs(i_path, "/raw/inventory")
    upload_to_hdfs(log_path, "/logs/exceptions")
    
    print("\n Data Generation & Ingestion Complete!")
