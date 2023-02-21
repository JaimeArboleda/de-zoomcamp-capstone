ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

COPY .pyenv .env
COPY gcp_credentials.json gcp_credentials.json

# Install dotenv and google cloud storage
RUN pip install python-dotenv
RUN pip install google-cloud-storage

# Install google connector
# WORKDIR ${SPARK_HOME}/jars/
USER root
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar -P ${SPARK_HOME}/jars/
WORKDIR $HOME