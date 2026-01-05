import psycopg2
import json
import csv
import random
import os
import subprocess
import shutil
import time
from datetime import datetime
from faker import Faker

# --- CONFIGURATION ---
DB_PARAMS = {
    "host": "localhost",
    "port": "5432",
    "database": "procurement_db",
    "user": "user",
    "password": "password"
}

ORDERS_PER_DAY = 500000  # High volume
DATE_TO_GENERATE = datetime.now().strftime("%Y-%m-%d")
LOCAL_OUTPUT_DIR = "./generated_data"
fake = Faker()

# --- MOROCCAN MASTER DATA ---
# (Same data as before...)
MOROCCAN_STORES = [
    ("STORE-CAS-01", "Marjane Californie", "Casablanca", 33.5552, -7.6366),
    ("STORE-CAS-02", "Morocco Mall", "Casablanca", 33.5956, -7.6989),
    ("STORE-RAB-01", "Marjane Hay Riad", "Rabat", 33.9604, -6.8653),
    ("STORE-TNG-01", "Socco Alto", "Tangier", 35.7595, -5.8340),
    ("STORE-MAR-01", "Menara Mall", "Marrakech", 31.6214, -8.0163),
    ("STORE-AGA-01", "Carrefour Agadir", "Agadir", 30.4278, -9.5981),
    ("STORE-FES-01", "Borj Fez", "Fes", 34.0456, -4.9966)
]

MOROCCAN_PRODUCTS = [
    ("PRD-001", "Sidi Ali 1.5L", 6.50),
    ("PRD-002", "Oulmes 1L", 7.00),
    ("PRD-003", "Couscous Dari 1kg", 13.50),
    ("PRD-004", "Thé Sultan Vert", 18.00),
    ("PRD-005", "Aicha Confiture Fraise", 22.00),
    ("PRD-006", "Lait Centrale Danone", 3.50),
    ("PRD-007", "Raibi Jamila", 2.50),
    ("PRD-008", "Huile d'Olive Al Horra", 65.00),
    ("PRD-009", "Fromage La Vache Qui Rit", 15.00),
    ("PRD-010", "Merendina", 2.00),
    ("PRD-011", "Pasta Tria", 8.00),
    ("PRD-012", "Sardines Titus", 5.50),
    ("PRD-013", "Coca-Cola 1L", 9.00),
    ("PRD-014", "Atay Sebou", 14.00),
    ("PRD-015", "Eau Ciel 5L", 12.00)
]

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def seed_database(conn):
    print("🇲🇦 Seeding Database with Moroccan Data...")
    with conn.cursor() as cur:
        # Clean up old tables
        cur.execute("DROP TABLE IF EXISTS purchase_orders CASCADE") # Cleanup if exists
        cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        cur.execute("DROP TABLE IF EXISTS stores CASCADE")
        cur.execute("DROP TABLE IF EXISTS products CASCADE")
        cur.execute("DROP TABLE IF EXISTS warehouses CASCADE")

        # Create Tables
        cur.execute("""
            CREATE TABLE stores (
                store_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                city VARCHAR(50),
                latitude DECIMAL(9,6),
                longitude DECIMAL(9,6)
            )
        """)
        for s in MOROCCAN_STORES:
            cur.execute("INSERT INTO stores (store_id, name, city, latitude, longitude) VALUES (%s, %s, %s, %s, %s)", s)

        cur.execute("""
            CREATE TABLE products (
                sku VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2)
            )
        """)
        for p in MOROCCAN_PRODUCTS:
            cur.execute("INSERT INTO products (sku, name, price) VALUES (%s, %s, %s)", p)

        cur.execute("CREATE TABLE warehouses (warehouse_id VARCHAR(50) PRIMARY KEY, location VARCHAR(100))")
        cur.execute("INSERT INTO warehouses VALUES ('WH-CAS-01', 'Casablanca Industrial Zone')")
        cur.execute("INSERT INTO warehouses VALUES ('WH-TNG-free', 'Tangier Free Zone')")

        conn.commit()
    print("✅ Database Seeded Successfully!")

def fetch_master_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT store_id FROM stores")
        store_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT warehouse_id FROM warehouses")
        wh_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT sku, price FROM products")
        products = {row[0]: float(row[1]) for row in cur.fetchall()}
    return store_ids, wh_ids, products

def generate_orders_partitioned(store_ids, products, date_str):
    print(f"🚀 Generating {ORDERS_PER_DAY} Orders (Partitioned by Store)...")
    
    base_path = f"{LOCAL_OUTPUT_DIR}/orders/dt={date_str}"
    if os.path.exists(base_path):
        shutil.rmtree(base_path)
    
    # Create file handles for each store to avoid opening/closing files 500k times
    files = {}
    for store_id in store_ids:
        path = f"{base_path}/store_id={store_id}"
        os.makedirs(path, exist_ok=True)
        files[store_id] = open(f"{path}/orders.json", "w", encoding="utf-8")

    product_keys = list(products.keys())

    try:
        for i in range(ORDERS_PER_DAY):
            store = random.choice(store_ids)
            order_id = f"ORD-{date_str.replace('-','')}-{fake.hexify(text='^^^^^^^^')}"
            
            num_items = random.choices([1, 2, 3, 5, 10], weights=[30, 30, 20, 15, 5], k=1)[0]
            selected_skus = random.sample(product_keys, num_items)
            
            items = []
            for sku in selected_skus:
                items.append({
                    "sku": sku,
                    "quantity": random.randint(1, 10),
                    "unit_price": products[sku]
                })
            
            order_record = {
                "order_id": order_id,
                # "store_id": store, <-- OPTIONAL: We don't strictly need this in the JSON anymore since it's in the folder name, but safe to keep.
                "timestamp": f"{date_str}T{fake.time()}",
                "items": items,
                # "dt": date_str <-- same here, redundant but harmless
            }
            files[store].write(json.dumps(order_record) + '\n')
            
            if i % 50000 == 0:
                print(f"   ... {i} orders generated")
    finally:
        # Close all files
        for f in files.values():
            f.close()

    return base_path

def generate_inventory(wh_ids, products, date_str):
    print(f"📦 Generating Inventory Snapshots for {date_str}...")
    # Inventory is small, we don't need to partition it by warehouse usually, but let's keep it simple (single file per day)
    path = f"{LOCAL_OUTPUT_DIR}/inventory/dt={date_str}"
    os.makedirs(path, exist_ok=True)
    filename = f"{path}/inventory.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["warehouse_id", "sku", "available_qty", "reserved_qty"])
        for wh in wh_ids:
            for sku in products.keys():
                avail = random.randint(0, 5000)
                reserved = random.randint(0, 200)
                writer.writerow([wh, sku, avail, reserved])
    return path

def upload_folder_to_hdfs(local_folder, hdfs_parent_path):
    """
    Uploads a directory structure recursively to HDFS.
    """
    folder_name = os.path.basename(local_folder) # e.g., dt=2025-12-30
    hdfs_target = f"{hdfs_parent_path}/{folder_name}"
    
    print(f"🚢 Uploading Folder {local_folder} -> {hdfs_target}...")

    # 1. Clean old data in HDFS if exists (to avoid duplicates)
    subprocess.run(f"docker exec namenode hdfs dfs -rm -r -f {hdfs_target}", shell=True, stderr=subprocess.DEVNULL)
    
    # 2. Create Parent Directory
    subprocess.run(f"docker exec namenode hdfs dfs -mkdir -p {hdfs_parent_path}", shell=True, check=True)

    # 3. Copy folder to container
    subprocess.run(f"docker cp {local_folder} namenode:/tmp/", shell=True, check=True)
    
    # 4. Put folder into HDFS
    subprocess.run(f"docker exec namenode hdfs dfs -put /tmp/{folder_name} {hdfs_parent_path}/", shell=True, check=True)
    
    # 5. Fix permissions (Recursive)
    subprocess.run(f"docker exec namenode hdfs dfs -chmod -R 777 {hdfs_parent_path}", shell=True, check=True)

    # 6. Cleanup
    subprocess.run(f"docker exec namenode rm -rf /tmp/{folder_name}", shell=True)
    print("✅ Upload Complete.")

if __name__ == "__main__":
    try:
        conn = get_db_connection()
        seed_database(conn)
        stores, warehouses, products = fetch_master_data(conn)
        conn.close()

        start_time = time.time()
        
        # Generate Partitioned Data
        orders_path = generate_orders_partitioned(stores, products, DATE_TO_GENERATE)
        inventory_path = generate_inventory(warehouses, products, DATE_TO_GENERATE)
        
        print(f"⏱️ Generation took {round(time.time() - start_time, 2)} seconds.")

        # Upload
        # NOTE: logic changed slightly. LOCAL_OUTPUT_DIR/orders/dt=... -> HDFS /raw/orders/dt=...
        upload_folder_to_hdfs(orders_path, "/raw/orders")
        upload_folder_to_hdfs(inventory_path, "/raw/inventory")

        print("\n🇲🇦 Phase 2 Complete: Partitioned Moroccan Data in HDFS!")

    except Exception as e:
        print(f"Error: {e}")