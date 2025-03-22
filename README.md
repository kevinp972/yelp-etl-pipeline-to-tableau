## Project Overview

### Project Goal

This project aims to implement a real-world data pipeline that performs a series of computational steps to take data from its raw form, store it, transform it, and ultimately serve it to a client-facing application. The end goal is to enable the execution of a single Unix command that automates the entire pipeline and produces all the data needed to power a Tableau dashboard.

Although we initially intended to work with a much larger dataset to simulate a true big data environment, we were limited by AWS Free Tier constraints. Similarly, we had hoped to connect Tableau directly to AWS for real-time querying, but were restricted by limitations in the Tableau Public version. Despite these trade-offs, the structure and execution of this project closely mimic real-world data workflows.

### Architecture Overview

Our project follows a modular pipeline structured across four key layers:

#### 1. Data Acquisition Layer (Extract)

We start by downloading the open Yelp dataset in JSON format from Yelp Open Dataset. This data is manually moved into the `/data` directory of our EC2 instance, simulating the extraction of raw data in a real-world scenario.

#### 2. Data Processing Layer (Extract & Transform)

This stage is handled by Apache Spark. Using `spark-job.py`, we read the raw JSON files, clean and transform the data (e.g., type casting, column construction), and write the processed output in Parquet format into the `/spark/output` directory.

#### 3. Data Serving Layer (Load)

We use DuckDB as our lightweight, file-based data warehouse. The `duckdb-job.sql` script ingests the Parquet files, performs additional pre-aggregations and joins, and stores the results into `.csv` files within the `/duckdb/db_output` directory. This layer ensures that client applications can access pre-processed and query-optimized data.

#### 4. Client Serving Layer

The final processed `.csv` files are downloaded locally and imported into Tableau to build a user-facing dashboard. While our current setup does not support live AWS-to-Tableau connections due to Tableau Public restrictions, the dashboard effectively showcases the full pipeline’s output.

## Instructions to run the pipeline

### Downloading the original data

We went to this page: https://business.yelp.com/data/resources/open-dataset/ and downloaded the open Yelp dataset in JSON format.

```
wget https://business.yelp.com/external-assets/files/Yelp-JSON.zip
unzip Yelp-JSON.zip
```

### Clone the code from github

Clone this repo containing our code:

```
git clone https://github.com/Prof-Rosario-UCLA/team17.git
```

**ATTENTION**: The original data directory should be empty and please move the data we wget and unzip from the yelp website into the data directory.

The file structure should be:

```
.
├── README.md
├── bash
│   └── pipeline.sh
├── data
│   ├── yelp_academic_dataset_business.json
│   ├── yelp_academic_dataset_checkin.json
│   └── yelp_academic_dataset_tip.json
├── duckdb
│   ├── README.md
│   ├── db_output
│   ├── duckdb-job.sql
│   └── yelp.db
└── spark
    ├── README.md
    ├── output
    ├── spark-job-demo.py
    └── spark-job.py
```

Note: yelp.db is currently not in the repo, and will be generated when running the pipeline.

### Run the pipeline

The pipeline consists of two steps: first, the execution of `spark-job.py`, followed by the execution of `duckdb-job.sql`. It includes message outputs and rollback functions for error handling.

- `spark-job.py`: Reading the 3 .json files from `~/team17/data`. Completing the data manipulation task. Writing the output as .parquet format into `~/team17/spark/output`.
- `duckdb-job.sql`: Reading the 3 .parquet file into tables. Completing the data pre-aggregation task and Creating corresponding tables in the database `~/team17/duckdb/yelp.db`. Writing the tables as .csv format into `~/team17/duckdb/db_output`.

After changing the data paths to the correct ones in `pipeline.sh`, run the following:

```
cd ~/team17/bash
bash pipeline.sh
```

### Download the data output

Once the .csv files are available, download them to your local computer for loading into Tableau. To do this, create a local directory, navigate to it in the console, and run the following command:

```
sshpass -p '[Insert Password Here]' scp anshadui@ec2-52-12-93-138.us-west-2.compute.amazonaws.com:/home/anshadui/team17/duckdb/db_output/*.csv ./
```

p.s. `sshpass` is a tool for automating SSH password entry in scripts, allowing password-based authentication without manual input. To install `sshpass`, run:

- On Ubuntu/Debian: `sudo apt install sshpass`
- On CentOS/RHEL: `sudo yum install sshpass`
- On macOS (Homebrew): `brew install hudochenkov/sshpass/sshpass`

## Connecting output to Tableau dashboard

The resulting Tableau dashboard can be accessed in the following link. 

https://public.tableau.com/app/profile/june.liu2832/viz/finalproject_17421027776650/Dashboard
