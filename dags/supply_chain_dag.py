from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# On importe tout depuis notre nouveau fichier utils.py
from utils import (
    seed_database, generate_and_process, upload_raw_to_hdfs, 
    setup_tables, run_trino_aggregation, generate_supplier_files, 
    upload_results_to_hdfs
)

default_args = {
    'owner': 'Khalil',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'supply_chain_pipeline',
    default_args=default_args,
    description='Pipeline End-to-End: Generation -> Ingestion -> Trino -> Output',
    schedule_interval='0 22 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # 1. Initialisation 
    t_seed = PythonOperator(
        task_id='seed_postgres_db',
        python_callable=seed_database
    )

    # 2. Génération des données (Simulation)
    def task_gen(**kwargs):
        generate_and_process(kwargs['ds']) # 'ds' = date d'exécution (YYYY-MM-DD)

    t_gen = PythonOperator(
        task_id='generate_data',
        python_callable=task_gen,
        provide_context=True
    )

    # 3. Ingestion HDFS (Raw)
    def task_up_raw(**kwargs):
        upload_raw_to_hdfs(kwargs['ds'])

    t_up_raw = PythonOperator(
        task_id='upload_raw_hdfs',
        python_callable=task_up_raw,
        provide_context=True
    )

    # 4. Setup Tables (Hive Metastore)
    t_setup = PythonOperator(
        task_id='setup_hive_tables',
        python_callable=setup_tables
    )

    # 5. Compute (Trino) + 6. Generation Commandes
    def task_compute_and_export(**kwargs):
        date_str = kwargs['ds']
        # Appel Trino
        results = run_trino_aggregation(date_str)
        # Génération fichiers JSON
        output_path = generate_supplier_files(results, date_str)
        # Upload final
        upload_results_to_hdfs(output_path, date_str)

    t_process = PythonOperator(
        task_id='compute_and_export',
        python_callable=task_compute_and_export,
        provide_context=True
    )

    # Orchestration
    t_seed >> t_gen >> t_up_raw >> t_setup >> t_process