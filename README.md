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
