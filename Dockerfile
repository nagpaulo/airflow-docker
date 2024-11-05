FROM apache/airflow:2.10.2
ADD requirements.txt .
ADD requirements-project.txt .
COPY files files
ADD gcp_key_file.json .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements-project.txt