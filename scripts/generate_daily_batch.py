import psycopg2
import json
import csv
import random
import os
import subprocess
from datetime import datetime, timedelta
from faker import Faker

# --- CONFIGURATION ---
DB_PARAMS = {
    "host": "localhost",
    "port": "5432",
    "database": "procurement_db",
    "user": "user",
    "password": "password"
}

# Generate data for TODAY
DATE_TO_GENERATE = datetime.now().strftime("%Y-%m-%d")
LOCAL_OUTPUT_DIR = "./generated_data"
fake = Faker()

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def fetch_master_data(conn):
    """Fetch valid IDs from Postgres so we don't generate invalid data."""
    print("Fetching Master Data from PostgreSQL...")
    with conn.cursor() as cur:
        cur.execute("SELECT store_id FROM stores")
        store_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT warehouse_id FROM warehouses")
        wh_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT sku, price FROM products")
        products = {row[0]: float(row[1]) for row in cur.fetchall()} # Map SKU -> Price
        
    return store_ids, wh_ids, products

def generate_orders(store_ids, products, date_str):
    """
    Generates daily POS orders in NDJSON format (Newline Delimited JSON).
    CRITICAL: This format is required for Hive/Trino. Do NOT use a JSON Array [].
    """
    print(f"Generating Orders for {date_str} (NDJSON format)...")
    filename = f"{LOCAL_OUTPUT_DIR}/orders_{date_str}.json"
    
    # Open with UTF-8 to prevent Windows encoding issues
    with open(filename, 'w', encoding='utf-8') as f:
        for _ in range(len(store_ids) * 5): # Avg 5 orders per store
            store = random.choice(store_ids)
            order_id = f"ORD-{date_str.replace('-','')}-{fake.uuid4()[:8]}"
            
            items = []
            num_items = random.randint(1, 10)
            selected_skus = random.sample(list(products.keys()), num_items)
            
            for sku in selected_skus:
                qty = random.randint(1, 20)
                items.append({
                    "sku": sku,
                    "quantity": qty,
                    "unit_price": products[sku]
                })
            
            order_record = {
                "order_id": order_id,
                "store_id": store,
                "timestamp": f"{date_str}T{fake.time()}",
                "items": items,
                "dt": date_str  # Redundant but useful for checks
            }

            # WRITE LINE BY LINE (NDJSON) - No commas, no brackets
            f.write(json.dumps(order_record) + '\n')
            
    return filename

def generate_inventory(wh_ids, products, date_str):
    """Generates daily Warehouse Inventory in CSV format."""
    print(f"Generating Inventory Snapshots for {date_str}...")
    filename = f"{LOCAL_OUTPUT_DIR}/inventory_{date_str}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Header is usually skipped in Hive table definition, but keeping it for clarity
        # (Make sure your Hive TBLPROPERTIES set "skip.header.line.count"="1")
        writer.writerow(["warehouse_id", "sku", "available_qty", "reserved_qty"])
        
        for wh in wh_ids:
            # Randomly select 80% of products to be 'in stock' at this warehouse
            stocked_products = random.sample(list(products.keys()), int(len(products) * 0.8))
            
            for sku in stocked_products:
                avail = random.randint(0, 1000)
                reserved = random.randint(0, 50)
                writer.writerow([wh, sku, avail, reserved])
                
    return filename

def upload_to_hdfs(local_path, base_folder, date_str):
    """
    Uses Docker commands to put file into HDFS.
    Uses Hive Partitioning structure: /path/dt=YYYY-MM-DD/file
    """
    filename = os.path.basename(local_path)
    
    # CRITICAL: Use Hive Partition Format (dt=YYYY-MM-DD)
    hdfs_folder = f"{base_folder}/dt={date_str}"
    hdfs_path = f"{hdfs_folder}/{filename}"
    
    print(f"Uploading {filename} to HDFS: {hdfs_path}...")
    
    try:
        # 1. Create Directory in HDFS (Partition Folder)
        subprocess.run(
            f"docker exec namenode hdfs dfs -mkdir -p {hdfs_folder}", 
            shell=True, check=True
        )
        
        # 2. Copy file to container (temporary)
        subprocess.run(
            f"docker cp {local_path} namenode:/tmp/{filename}", 
            shell=True, check=True
        )
        
        # 3. Move from container temp to HDFS
        # Using -f (force) to overwrite if exists
        subprocess.run(
            f"docker exec namenode hdfs dfs -put -f /tmp/{filename} {hdfs_path}", 
            shell=True, check=True
        )
        
        # 4. FIX PERMISSIONS (Crucial for Trino)
        # Since root uploads it, Trino (user 'trino') needs read access
        subprocess.run(
            f"docker exec namenode hdfs dfs -chmod 777 {hdfs_path}", 
            shell=True, check=True
        )

        # 5. Cleanup temp file in container
        subprocess.run(f"docker exec namenode rm /tmp/{filename}", shell=True)
        
        print(f"✅ Upload Successful: {hdfs_path}")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to upload to HDFS: {e}")

if __name__ == "__main__":
    # Ensure output directory exists
    if not os.path.exists(LOCAL_OUTPUT_DIR):
        os.makedirs(LOCAL_OUTPUT_DIR)

    try:
        conn = get_db_connection()
        stores, warehouses, products = fetch_master_data(conn)
        conn.close()

        # 1. Generate Files
        orders_file = generate_orders(stores, products, DATE_TO_GENERATE)
        inventory_file = generate_inventory(warehouses, products, DATE_TO_GENERATE)

        # 2. Upload to HDFS with Partitioning
        # Note: We pass the BASE folder. The function adds /dt=...
        upload_to_hdfs(orders_file, "/raw/orders", DATE_TO_GENERATE)
        upload_to_hdfs(inventory_file, "/raw/inventory", DATE_TO_GENERATE)

        print("\n🎉 Batch Complete! Files are now in HDFS with correct format and permissions.")

    except Exception as e:
        print(f"Error: {e}")