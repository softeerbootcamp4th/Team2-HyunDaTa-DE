FROM apache/airflow:2.9.3

COPY requirements.txt .
COPY config/* /opt/airflow/config/
RUN pip install --no-cache-dir -r requirements.txt