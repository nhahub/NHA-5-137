FROM apache/airflow:3.3.2

# Switch to airflow user to install python packages
USER airflow

# Copy requirements and install AWS provider and dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
