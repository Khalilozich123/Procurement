Write-Host "🚀 Starting Project Setup for Partners..." -ForegroundColor Green

# 1. Start everything (using the fixed configs from GitHub)
Write-Host "1. Launching Docker Containers..."
docker-compose up -d

# 2. Wait for services to be ready
Write-Host "⏳ Waiting 30 seconds for Trino and Hadoop to wake up..."
Start-Sleep -Seconds 30

# 3. Create HDFS Structure
Write-Host "2. Creating HDFS Folders..."
docker exec namenode hdfs dfs -mkdir -p /raw/orders
docker exec namenode hdfs dfs -mkdir -p /raw/inventory
docker exec namenode hdfs dfs -mkdir -p /processed
# Open permissions so Trino can write to them
docker exec namenode hdfs dfs -chmod -R 777 /raw
docker exec namenode hdfs dfs -chmod -R 777 /processed

# 4. Define Tables in Trino
Write-Host "3. defining Hive Tables..."

# We execute the SQL directly via Trino CLI
$sql_orders = "CREATE TABLE IF NOT EXISTS hive.default.raw_orders (order_id VARCHAR, store_id VARCHAR, timestamp VARCHAR, items ARRAY<ROW(sku VARCHAR, quantity INT, unit_price DOUBLE)>, dt VARCHAR) WITH (format = 'JSON', external_location = 'hdfs://namenode:9000/raw/orders', partitioned_by = ARRAY['dt']);"
$sql_inventory = "CREATE TABLE IF NOT EXISTS hive.default.raw_inventory (warehouse_id VARCHAR, sku VARCHAR, available_qty VARCHAR, reserved_qty VARCHAR, dt VARCHAR) WITH (format = 'CSV', skip_header_line_count = 1, external_location = 'hdfs://namenode:9000/raw/inventory', partitioned_by = ARRAY['dt']);"
$sql_sync = "CALL hive.system.sync_partition_metadata('default', 'raw_orders', 'ADD'); CALL hive.system.sync_partition_metadata('default', 'raw_inventory', 'ADD');"

# Run the queries
docker exec -i trino trino --execute "$sql_orders"
docker exec -i trino trino --execute "$sql_inventory"
docker exec -i trino trino --execute "$sql_sync"

Write-Host "✅ Setup Complete! You can now run 'python generate_daily_batch.py'" -ForegroundColor Green