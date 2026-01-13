from trino.dbapi import connect
import time

# --- CONFIGURATION ---
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "admin"

def run_ddl(cur, query):
    try:
        cur.execute(query)
        print("✅ Success.")
    except Exception as e:
        print(f"❌ Error: {e}")

def setup_tables():
    print("🔌 Connecting to Trino...")
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="hive", schema="default")
    cur = conn.cursor()

    # 1. Create Schema
    run_ddl(cur, "CREATE SCHEMA IF NOT EXISTS hive.default")

    # 2. Create Orders Table (JSON)
    # >>> FIX: Added store_id as a PARTITION column
    print("\n--- 2. Creating 'raw_orders' Table ---")
    run_ddl(cur, "DROP TABLE IF EXISTS hive.default.raw_orders")
    
    create_orders = """
    CREATE TABLE hive.default.raw_orders (
        order_id VARCHAR,
        timestamp VARCHAR,
        items ARRAY(ROW(sku VARCHAR, quantity INT, unit_price DOUBLE)),
        dt VARCHAR,       -- Partition 1
        store_id VARCHAR  -- Partition 2 (The Missing Column!)
    )
    WITH (
        format = 'JSON',
        external_location = 'hdfs://namenode:9000/raw/orders/',
        partitioned_by = ARRAY['dt', 'store_id']
    )
    """
    run_ddl(cur, create_orders)

    # 3. Create Inventory Table (CSV)
    print("\n--- 3. Creating 'raw_inventory' Table ---")
    run_ddl(cur, "DROP TABLE IF EXISTS hive.default.raw_inventory")
    
    create_inventory = """
    CREATE TABLE hive.default.raw_inventory (
        warehouse_id VARCHAR,
        sku VARCHAR,
        available_qty VARCHAR,
        reserved_qty VARCHAR,
        dt VARCHAR
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/inventory/',
        partitioned_by = ARRAY['dt']
    )
    """
    run_ddl(cur, create_inventory)

    # 4. Sync Partitions
    print("\n--- 4. Registering Partitions ---")
    try:
        # We must sync to discover both 'dt' and 'store_id' folders
        cur.execute("CALL system.sync_partition_metadata('default', 'raw_orders', 'FULL')")
        print("✅ Orders Partitions Synced.")
        cur.execute("CALL system.sync_partition_metadata('default', 'raw_inventory', 'FULL')")
        print("✅ Inventory Partitions Synced.")
    except Exception as e:
        print(f"⚠️ Warning during sync: {e}")

    print("\n🎉 Tables Fixed! Dashboard should work now.")

if __name__ == "__main__":
    setup_tables()