import os
import json
import csv
import random
import subprocess
import shutil
import psycopg2
from datetime import datetime
from faker import Faker
from trino.dbapi import connect

# --- 1. CONFIGURATION CENTRALISÉE ---

# Détection de l'environnement (Docker ou Local)
def get_host(service_name):
    if os.path.exists('/.dockerenv'): return service_name
    return "localhost"

DB_PARAMS = {
    "host": get_host("postgres"),
    "port": "5432",
    "database": "procurement_db",
    "user": "user",
    "password": "password"
}

TRINO_HOST = get_host("trino")
TRINO_PORT = 8080
TRINO_USER = "admin"

# Chemins : On utilise des chemins absolus pour Airflow
AIRFLOW_DATA_DIR = "/opt/airflow/generated_data"

fake = Faker()

# --- 2. DONNÉES DE RÉFÉRENCE (CONSTANTES) ---
SUPPLIERS = [
    ("SUP-001", "Les Eaux Minérales d'Oulmès", "Casablanca"),
    ("SUP-002", "Centrale Danone", "Casablanca"),
    ("SUP-003", "Dari Couspate", "Salé"),
    ("SUP-004", "Cosumar", "Casablanca"),
    ("SUP-005", "Dislog Group", "Casablanca")
]

MOROCCAN_PRODUCTS = [
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

# --- 3. FONCTIONS UTILITAIRES ---

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def seed_database():
    """Initialise la BDD Postgres (Reset)"""
    conn = get_db_connection()
    print("Seeding Database...")
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
        cur.execute("CREATE TABLE stores (store_id VARCHAR(50) PRIMARY KEY, name VARCHAR(100))")
        
        for s in STORES: 
            cur.execute("INSERT INTO warehouses VALUES (%s, %s)", (f"WH-{s[0]}", s[0]))
            cur.execute("INSERT INTO stores VALUES (%s, %s)", (s[0], s[1]))
            
        conn.commit()
    conn.close()

def fetch_products_and_stores():
    """Récupère les données simples pour la génération"""
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT p.sku, p.name, p.price FROM products p")
        products = {row[0]: {"name": row[1], "price": float(row[2])} for row in cur.fetchall()}
        
        cur.execute("SELECT store_id FROM warehouses")
        stores = [row[0].replace("WH-", "") for row in cur.fetchall()]
    conn.close()
    return products, stores

def fetch_replenishment_rules():
    """Récupère les données riches pour le calcul"""
    conn = get_db_connection()
    with conn.cursor() as cur:
        query = """
        SELECT p.sku, p.name, p.supplier_id, s.name, r.safety_stock, r.moq 
        FROM products p 
        JOIN suppliers s ON p.supplier_id = s.supplier_id 
        JOIN replenishment_rules r ON p.sku = r.sku
        """
        cur.execute(query)
        data = {row[0]: {"name": row[1], "sup_id": row[2], "sup_name": row[3], "safety": row[4], "moq": row[5]} for row in cur.fetchall()}
    conn.close()
    return data

# --- 4. FONCTIONS METIERS (ETAPES DU DAG) ---

def generate_and_process(date_str):
    """Génère les fichiers JSON et CSV"""
    products, stores = fetch_products_and_stores()
    
    # Nettoyage préventif
    if os.path.exists(AIRFLOW_DATA_DIR):
        # On ne supprime pas tout brutalement car Airflow peut avoir d'autres dossiers
        pass 
    else:
        os.makedirs(AIRFLOW_DATA_DIR)
        
    # --- Generation Logic ---
    base_path_orders = f"{AIRFLOW_DATA_DIR}/orders/dt={date_str}"
    files = {}
    sales_counts = {sku: 0 for sku in products}
    
    for sid in stores:
        p = f"{base_path_orders}/store_id={sid}"
        os.makedirs(p, exist_ok=True)
        files[sid] = open(f"{p}/orders.json", "w", encoding="utf-8")

    skus = list(products.keys())
    # Generation simple pour la démo
    for i in range(5000): # ORDERS_PER_DAY
        store = random.choice(stores)
        items = []
        for _ in range(random.randint(1, 3)):
            sku = random.choice(skus)
            qty = random.randint(1, 5)
            sales_counts[sku] += qty
            items.append({"sku": sku, "quantity": qty, "unit_price": products[sku]['price']})
        
        order = {"order_id": fake.uuid4(), "timestamp": f"{date_str}T{fake.time()}", "items": items}
        files[store].write(json.dumps(order) + '\n')
    
    for f in files.values(): f.close()

    # --- Inventory Logic ---
    base_path_inv = f"{AIRFLOW_DATA_DIR}/inventory/dt={date_str}"
    os.makedirs(base_path_inv, exist_ok=True)
    
    with open(f"{base_path_inv}/inventory.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["warehouse_id", "sku", "available_qty", "reserved_qty"])
        for store in stores:
            wh_id = f"WH-{store}"
            for sku in skus:
                store_sales_share = sales_counts[sku] // len(stores)
                start_stock = max(0, store_sales_share + random.randint(-5, 50))
                writer.writerow([wh_id, sku, start_stock, 0])

    print(f"Generated data for {date_str} in {AIRFLOW_DATA_DIR}")

def upload_raw_to_hdfs(date_str):
    """Upload les commandes et l'inventaire (Ingestion)"""
    # 1. Orders
    local_orders = f"{AIRFLOW_DATA_DIR}/orders/dt={date_str}"
    target_orders = f"/raw/orders/dt={date_str}"
    
    # Clean & Upload
    subprocess.run(f"docker exec namenode hdfs dfs -rm -r -f {target_orders}", shell=True)
    subprocess.run(f"docker exec namenode hdfs dfs -mkdir -p /raw/orders", shell=True)
    
    # Astuce: On copie via un tmp dans le conteneur
    tmp_path = f"/tmp/orders_{date_str}"
    subprocess.run(f"docker exec namenode rm -rf {tmp_path}", shell=True)
    subprocess.run(f"docker cp \"{local_orders}\" namenode:\"{tmp_path}\"", shell=True)
    subprocess.run(f"docker exec namenode hdfs dfs -put \"{tmp_path}\" /raw/orders/dt={date_str}", shell=True)
    
    # 2. Inventory
    local_inv = f"{AIRFLOW_DATA_DIR}/inventory/dt={date_str}"
    tmp_inv = f"/tmp/inv_{date_str}"
    
    subprocess.run(f"docker exec namenode hdfs dfs -mkdir -p /raw/inventory", shell=True)
    subprocess.run(f"docker exec namenode rm -rf {tmp_inv}", shell=True)
    subprocess.run(f"docker cp \"{local_inv}\" namenode:\"{tmp_inv}\"", shell=True)
    subprocess.run(f"docker exec namenode hdfs dfs -put -f \"{tmp_inv}\" /raw/inventory/dt={date_str}", shell=True)
    
    print("Upload Raw Data Complete.")

def setup_tables():
    """Crée les tables externes dans Trino/Hive"""
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="hive", schema="default")
    cur = conn.cursor()
    
    cur.execute("CREATE SCHEMA IF NOT EXISTS hive.default")
    cur.execute("DROP TABLE IF EXISTS hive.default.raw_orders")
    
    create_orders = """
    CREATE TABLE hive.default.raw_orders (
        order_id VARCHAR, timestamp VARCHAR,
        items ARRAY(ROW(sku VARCHAR, quantity INT, unit_price DOUBLE)),
        dt VARCHAR, store_id VARCHAR
    ) WITH (
        format = 'JSON', external_location = 'hdfs://namenode:9000/raw/orders/',
        partitioned_by = ARRAY['dt', 'store_id']
    )
    """
    cur.execute(create_orders)
    
    cur.execute("DROP TABLE IF EXISTS hive.default.raw_inventory")
    create_inv = """
    CREATE TABLE hive.default.raw_inventory (
        warehouse_id VARCHAR, sku VARCHAR, available_qty VARCHAR, reserved_qty VARCHAR, dt VARCHAR
    ) WITH (
        format = 'CSV', skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/inventory/',
        partitioned_by = ARRAY['dt']
    )
    """
    cur.execute(create_inv)
    conn.close()

def run_trino_aggregation(date_str):
    """Exécute le calcul agrégé sur Trino"""
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="hive", schema="default")
    cur = conn.cursor()
    
    # Sync Partitions
    cur.execute("CALL system.sync_partition_metadata('default', 'raw_orders', 'FULL')")
    cur.execute("CALL system.sync_partition_metadata('default', 'raw_inventory', 'FULL')")
    
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
    return cur.fetchall()

def generate_supplier_files(trino_results, date_str):
    """Génère les JSON de commande fournisseur"""
    master_data = fetch_replenishment_rules()
    
    output_dir = f"{AIRFLOW_DATA_DIR}/supplier_orders/{date_str}"
    if os.path.exists(output_dir): shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    
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
                "sku": sku, "product": info['name'],
                "net_demand": net_demand, "final_order_quantity": qty_to_order
            })
            
    for sup, items in supplier_batches.items():
        filename = f"Order_{sup.replace(' ', '_')}_{date_str}.json"
        with open(f"{output_dir}/{filename}", "w") as f:
            json.dump({"supplier": sup, "date": date_str, "items": items}, f, indent=2)
            
    return output_dir

def upload_results_to_hdfs(local_dir, date_str):
    """Upload les résultats finaux"""
    target_dir = f"/output/supplier_orders/{date_str}"
    
    subprocess.run(f"docker exec namenode hdfs dfs -rm -r -f {target_dir}", shell=True)
    subprocess.run(f"docker exec namenode hdfs dfs -mkdir -p /output/supplier_orders", shell=True)
    
    tmp_path = f"/tmp/output_{date_str}"
    subprocess.run(f"docker exec namenode rm -rf {tmp_path}", shell=True)
    subprocess.run(f"docker cp \"{local_dir}\" namenode:\"{tmp_path}\"", shell=True)
    subprocess.run(f"docker exec namenode hdfs dfs -put \"{tmp_path}\" /output/supplier_orders/{date_str}", shell=True)
    
    print("Upload Results Complete.")