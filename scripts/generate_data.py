import psycopg2
from faker import Faker
import random
import csv
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DB_PARAMS = {
    "host": "localhost",
    "port": "5432",
    "database": "procurement_db",
    "user": "user",
    "password": "password"
}

# Quantities to generate
NUM_SUPPLIERS = 10
NUM_PRODUCTS = 50
NUM_WAREHOUSES = 2
NUM_STORES = 5

fake = Faker()

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def clean_data(conn):
    """Wipes existing data to start fresh."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE items, orders, inventory, products, suppliers, stores, warehouses CASCADE;")
        conn.commit()
        print("Existing data cleared.")

def generate_warehouses(conn):
    warehouses = []
    print(f"Generating {NUM_WAREHOUSES} Warehouses...")
    with conn.cursor() as cur:
        for i in range(1, NUM_WAREHOUSES + 1):
            wh_id = f"WH-{i:03d}"
            name = f"Depot {fake.city()}"
            loc = fake.address()
            w_type = random.choice(["Regional DC", "Local Hub", "Cold Storage"])
            
            cur.execute(
                "INSERT INTO warehouses (warehouse_id, name, location, type) VALUES (%s, %s, %s, %s)",
                (wh_id, name, loc, w_type)
            )
            warehouses.append(wh_id)
        conn.commit()
    return warehouses

def generate_suppliers(conn):
    suppliers = []
    print(f"Generating {NUM_SUPPLIERS} Suppliers...")
    with conn.cursor() as cur:
        for i in range(1, NUM_SUPPLIERS + 1):
            sup_id = f"SUP-{i:03d}"
            name = fake.company()
            email = fake.company_email()
            country = fake.country()
            
            cur.execute(
                "INSERT INTO suppliers (supplier_id, name, contact_email, country) VALUES (%s, %s, %s, %s)",
                (sup_id, name, email, country)
            )
            suppliers.append(sup_id)
        conn.commit()
    return suppliers

def generate_products(conn, supplier_ids):
    products = []
    categories = ["Dairy", "Beverages", "Snacks", "Household", "Bakery", "Produce"]
    print(f"Generating {NUM_PRODUCTS} Products...")
    
    with conn.cursor() as cur:
        for i in range(1, NUM_PRODUCTS + 1):
            sku = f"PROD-{i:04d}"
            name = f"{fake.word().capitalize()} {random.choice(['Delight', 'Pack', 'Fresh', 'Premium'])}"
            cat = random.choice(categories)
            price = round(random.uniform(1.50, 50.00), 2)
            sup_id = random.choice(supplier_ids)
            
            # Procurement Rules
            pack_size = random.choice([6, 12, 24, 50])
            moq = random.choice([10, 50, 100])
            lead_time = random.randint(1, 14) # Days
            safety_stock = random.randint(10, 100)
            
            cur.execute(
                """
                INSERT INTO products (sku, name, category, price, supplier_id, pack_size, moq, lead_time_days, safety_stock)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (sku, name, cat, price, sup_id, pack_size, moq, lead_time, safety_stock)
            )
            products.append(sku)
        conn.commit()
    return products

def generate_stores(conn, warehouse_ids):
    stores = []
    print(f"Generating {NUM_STORES} Stores...")
    with conn.cursor() as cur:
        for i in range(1, NUM_STORES + 1):
            store_id = f"STORE-{i:03d}"
            name = f"Supermarket {fake.street_name()}"
            city = fake.city()
            wh_id = random.choice(warehouse_ids) # Assign a parent warehouse
            
            cur.execute(
                "INSERT INTO stores (store_id, name, city, warehouse_id) VALUES (%s, %s, %s, %s)",
                (store_id, name, city, wh_id)
            )
            stores.append(store_id)
        conn.commit()
    return stores

if __name__ == "__main__":
    try:
        conn = get_db_connection()
        # Optional: Clean old data if re-running
        # clean_data(conn) 
        
        wh_ids = generate_warehouses(conn)
        sup_ids = generate_suppliers(conn)
        generate_products(conn, sup_ids)
        generate_stores(conn, wh_ids)
        
        print("✅ Master Data Generation Complete!")
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")