# Introduction

# Objectives

# Techologies used

# Reproducing the results

## Prerequisites 

### Local prerequisites

Docker, Python 3.8 and git.

### Remote prerequisites

Google Cloud Project with a Bucket in GCS named "my_files" and a Database in BigQuery. 

Prefect Cloud account.

Simfim account.

## Instructions

1. Git clone the repository: 

    ```
    git clone 
    ```

2. Create a virtual environment and install requirements:

    ```
    python -m venv venv
    pip install -r requirements.txt
    ```

3. Register the GCP blocks in prefect with the CLI:
   
   ```
   prefect block register -m prefect_gcp
   ```

4. Create a file for env variables named ".env" at the root and put there the API KEY of simfim.
 TODO: add all config items

5. Download the JSON credentials file and put it at the root with the name "gcp_credentials.json".

6. Logged in on your Prefect Cloud account, run init blocks:

    ```
    python ./init_blocks.py
    ```

7. Run extract flow:
   
   ``` 
   python ./flows/extract.py
   ```