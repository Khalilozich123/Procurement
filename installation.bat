@echo off
echo ===================================================
echo   🦅 STARTING PROCUREMENT PIPELINE INSTALLATION
echo ===================================================

REM 1. Start Docker Containers
echo.
echo [1/4] 🐳 Starting Docker Infrastructure...
docker-compose down
docker-compose up -d --build
if %errorlevel% neq 0 (
    echo ❌ Docker failed to start. Is Docker Desktop running?
    pause
    exit /b
)

REM 2. Wait for Services to Warm Up
echo.
echo [2/4] ⏳ Waiting 45 seconds for Postgres and Trino to initialize...
echo      (This ensures the databases are ready to accept connections)
timeout /t 45 /nobreak >nul

REM 3. Seed Database & Generate Initial Data
echo.
echo [3/4] Creating Postgres Tables & Seeding Master Data...
echo      (Running script inside the 'scheduler' container)
docker exec scheduler python scripts/generate_orders.py
if %errorlevel% neq 0 (
    echo ❌ Database seeding failed!
    pause
    exit /b
)

REM 4. Setup Trino Tables
echo.
echo [4/4] 🚀 Registering Hive Tables in Trino...
docker exec scheduler python scripts/setup_trino.py
if %errorlevel% neq 0 (
    echo ❌ Trino setup failed!
    pause
    exit /b
)

echo.
echo ===================================================
echo    INSTALLATION COMPLETE!
echo ===================================================
echo.
echo 1. HDFS Navigator is at: http://localhost:9870
echo 2. The 'scheduler' container is now running in the background.
echo    It will automatically run the pipeline every day at 22:00.
echo.
pause