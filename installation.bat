@echo off
echo ===================================================
echo   🦅 STARTING CLEAN INSTALLATION (HOST MODE)
echo ===================================================

REM 1. Stop Docker & Remove Volumes (Releases file locks so we can delete folders)
echo.
echo [1/7]  Stopping Docker and removing old volumes...
docker compose down -v

REM 2. Delete Data Folders (The "Hard Reset")
echo.
echo [2/7]  Deleting old data folders (generated_data, hadoop_data)...
if exist "generated_data" rmdir /s /q "generated_data"
if exist "hadoop_data" rmdir /s /q "hadoop_data"

REM 3. Rebuild & Start
echo.
echo [3/7]  Building and Starting Docker Containers...
docker compose up -d --build
if %errorlevel% neq 0 (
    echo  Docker failed to start. Is Docker Desktop running?
    pause
    exit /b
)

REM 4. WAIT for Database Initialization 
echo.
echo ⏳ Waiting 45 seconds for Postgres & Trino to wake up...
echo    (If we don't wait, the Python scripts will fail to connect)
timeout /t 45 /nobreak >nul

echo.
echo [4/7]  Installing Python libraries...
pip install faker trino psycopg2 pandas

REM 6. Run Setup Script
echo.
echo [5/7]  Running Trino Setup...
python scripts/setup_trino.py

REM 7. Run Data Generation
echo.
echo [6/7]  Generating Orders...
REM 
python scripts/generate_orders.py

REM 8. Run Compute Demand
echo.
echo [7/7]  Computing Demand...
REM 
python scripts/compute_demand.py

echo.
echo ===================================================
echo    INSTALLATION COMPLETE!
echo ===================================================
pause