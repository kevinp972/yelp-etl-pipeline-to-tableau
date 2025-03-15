# Yelp Data Analysis with DuckDB

## 📌 Overview
This project processes the **Yelp Open Dataset** using **DuckDB**, performing data transformations and aggregations to generate **business insights** and **check-in trends** for visualization in **Tableau**. The script:
- Loads **business, check-in, and tip data** from Parquet files.
- Cleans and aggregates data to generate **key insights**.
- Exports results to **CSV** for visualization.

---

## 📂 Data Sources
The input data consists of **three Parquet files**, stored in:/home/anshadui/team17/spark/output/

| File Name | Description |
|-----------|-------------|
| **business_df.parquet** | Business information: name, rating, location, cuisine type. |
| **checkin_df.parquet** | Timestamped check-in data for each business. |
| **tip_df.parquet** | Short user-written reviews with timestamps and popularity scores. |

---

## 📊 Generated Tables
This script creates **multiple tables** to analyze business performance and customer interactions. The **key transformations** are:

### 1️⃣ Business Data Analysis
| Table | Description |
|-------|-------------|
| `graph1_business_summary` | Counts restaurant locations and calculates the average rating (**for restaurants with >50 locations**). |
| `graph2_state_summary` | Computes total businesses and average rating per **state**. |
| `graph2_city_summary` | Standardizes city names (**removes extra spaces & case differences**), calculates business count and average rating per **city**. |
| `graph3_top_rated_cities` | Identifies top-rated **cities with >50 businesses**, sorted by **highest average rating**. |
| `graph4_cuisine_summary` | Counts businesses per **cuisine type**. |
| `graph5_cuisinetip_summary` | Joins business and tip data to count **businesses and total reviews per cuisine type**. |

### 2️⃣ Check-in Trends
| Table | Description |
|-------|-------------|
| `graph6_checkin_aggregates_all` | Aggregates check-ins by **year, month, and year-month**. |
| `graph7a_checkin_num_res` | Counts check-ins per **restaurant per hour**. |
| `graph7b_checkin_num_all` | Counts total check-ins per **hour** across all businesses. |
| `graph8_city_checkin_num` | Counts **total check-ins per city**. |
| `graph9_restaurant_checkin_num` | Counts **total check-ins per restaurant**. |

### 3️⃣ Business Mapping
| Table | Description |
|-------|-------------|
| `graph10_businessmap` | Extracts **business locations** for mapping, **excluding businesses from states HI, AB, XMS**. |

---

## 📤 Output Files (CSV for Visualization)
After processing, the script **exports results as CSV files** to:/home/anshadui/team17/duckdb/db_output/


| Table | Output File |
|--------|------------|
| `graph1_business_summary` | `graph1_business_summary.csv` |
| `graph2_city_summary` | `graph2_city_summary.csv` |
| `graph2_state_summary` | `graph2_state_summary.csv` |
| `graph3_top_rated_cities` | `graph3_top_rated_cities.csv` |
| `graph4_cuisine_summary` | `graph4_cuisine_summary.csv` |
| `graph5_cuisinetip_summary` | `graph5_cuisinetip_summary.csv` |
| `graph6_checkin_aggregates_all` | `graph6_checkin_aggregates_all.csv` |
| `graph7a_checkin_num_res` | `graph7a_checkin_num_res.csv` |
| `graph7b_checkin_num_all` | `graph7b_checkin_num_all.csv` |
| `graph8_city_checkin_num` | `graph8_city_checkin_num.csv` |
| `graph9_restaurant_checkin_num` | `graph9_restaurant_checkin_num.csv` |
| `graph10_businessmap` | `graph10_businessmap.csv` |

---

