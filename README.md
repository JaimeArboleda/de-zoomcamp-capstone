# Data Engineering Zoomcamp 2023 Capstone

This repository contains my capstone project for the amazing Data Engineering Zoomcamp course (2023 edition).

## Objectives

The goals of the project are: 
* Collect finantial data from the US stock market (Extract).
* Process the finantial data to generate derived quantities for further analysis (Transform).
* Save the transformed data in Big Query (Load).
* Orchestrate the full pipeline using an orchestration tool. 
* Generate a report summarizing some of the findings. 

## Techologies 

We have used the following technologies: 
* Terraform for generating all the needed resources in Google Cloud. 
* Prefect for orchestrating the whole data pipeline.
* Spark for transforming the data. 
* Docker for simplifying the installation and usage of Spark. 
* Google Cloud Storage as a Data Lake. 
* Big Query as a Data Warehouse. 

## Components of the project

### Local prerequisites

For reproducibility, the PC must have installed Docker, Python (at least 3.8), git and Terraform.

After that, just clone the repository: 

```bash
git clone 
```

### Remote prerequisites

For reproducibility, a Google Cloud Project is needed. In addition, a free account in simfin is required (for obtaining the data).

### Setting the environment

One of the objectives (albeit not explicitily stated) is to achieve as much automation as possible, without repeating configuration options everywhere (that is, applying the DRY principle). In order to achieve this goal, the only two configuration steps that are required are: 

1. 

### Building the

1. Terraform

    ```
    cd terraform
    terraform init
    ```

    ```
    terraform apply 
    ```


2. Git clone the repository: 

    ```
    git clone 
    ```

3. Create a virtual environment and install requirements:

    ```
    python -m venv venv
    pip install -r requirements.txt
    ```

4. Register the GCP blocks in prefect with the CLI:
   
   ```
   prefect block register -m prefect_gcp
   ```

5. Create a file for env variables named ".env" at the root and put there the API KEY of simfim.
 TODO: add all config items

1. Download the JSON credentials file and put it at the root with the name "gcp_credentials.json".

2. Logged in on your Prefect Cloud account, run init blocks:

    ```
    python ./init_blocks.py
    ```

3. Run extract flow:
   
   ``` 
   python ./flows/extract.py
   ```

4. Run transform flow


Report: 


https://lookerstudio.google.com/reporting/1ec53650-f26e-40bb-be97-868dc352da57/page/tEnnC