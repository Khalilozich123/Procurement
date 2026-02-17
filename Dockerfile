FROM apache/airflow:2.7.1

# 1. Switch to root for system-level installs
USER root

# 2. Install Docker client (to run docker commands from Airflow)
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean

# 3. Switch to airflow user BEFORE pip install (required by Airflow base image)
USER airflow

# 4. Install missing Python libraries
RUN pip install --no-cache-dir faker trino psycopg2-binary

# 5. Stay as root so Airflow can access the Docker socket
USER root