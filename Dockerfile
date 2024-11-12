FROM apache/airflow:latest
ADD requirements.txt .
COPY files .
ADD gcp_key_file.json .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt