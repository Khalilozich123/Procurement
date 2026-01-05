FROM python:3.9-slim

# Install Docker CLI AND Timezone Data (tzdata)
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    lsb-release \
    tzdata \ 
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install psycopg2-binary faker

COPY scripts/scheduler.py .
COPY scripts/generate_orders_bulk.py .

CMD ["python", "-u", "scheduler.py"]