import schedule
import time
import subprocess
import os
from datetime import datetime

def run_pipeline():
    print(f"\n It is 22:00! Starting Daily Pipeline for {datetime.now().strftime('%Y-%m-%d')}...")
    
    try:
        # 1. Generate Orders
        print("- Running: generate_orders_bulk.py")
        subprocess.run(["python", "scripts/generate_orders.py"], check=True)
        
        # 2. Compute Demand
        print("- Running: compute_demand_trino.py")
        subprocess.run(["python", "scripts/compute_demand.py"], check=True)
        
        print(" Pipeline Finished Successfully.\n")
        
    except subprocess.CalledProcessError as e:
        print(f" Pipeline Failed! Error in script: {e}")

# Schedule the job
# (You can change "22:00" to a test time like "10:05" to see it work immediately)
schedule.every().day.at("21:00").do(run_pipeline)

print(" Orchestrator Started. Waiting for 22;00...")

while True:
    schedule.run_pending()
    time.sleep(60)