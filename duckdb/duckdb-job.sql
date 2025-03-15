DROP TABLE IF EXISTS business_origin;
DROP TABLE IF EXISTS tip_origin;
DROP TABLE IF EXISTS checkin_origin;

--

CREATE TABLE business_origin AS
SELECT * FROM read_parquet('/home/anshadui/team17/spark/output/business_df.parquet/\*.parquet');

CREATE TABLE checkin_origin AS
SELECT * FROM read_parquet('/home/anshadui/team17/spark/output/checkin_df.parquet/\*.parquet');

CREATE TABLE tip_origin AS
SELECT * FROM read_parquet('/home/anshadui/team17/spark/output/tip_df.parquet/\*.parquet');

--

DROP TABLE IF EXISTS graph1_business_summary;
DROP TABLE IF EXISTS graph2_city_summary;
DROP TABLE IF EXISTS graph2_state_summary;
DROP TABLE IF EXISTS graph3_top_rated_cities;
DROP TABLE IF EXISTS graph4_cuisine_summary;
DROP TABLE IF EXISTS graph5_cuisinetip_summary;
DROP TABLE IF EXISTS graph6_checkin_aggregates_all;
DROP TABLE IF EXISTS graph7a_checkin_num_res;
DROP TABLE IF EXISTS graph7b_checkin_num_all;
DROP TABLE IF EXISTS graph8_city_checkin_num;
DROP TABLE IF EXISTS graph9_restaurant_checkin_num;
DROP TABLE IF EXISTS graph10_businessmap;

-- Perform business data analysis
CREATE TABLE graph1_business_summary AS
    SELECT
        restaurant_name,
        COUNT(business_id) AS num_locations,
        ROUND(AVG(rating), 2) AS avg_rating
    FROM business_origin
    WHERE restaurant_name IS NOT NULL
    GROUP BY restaurant_name
    HAVING COUNT(business_id) > 50
    ORDER BY num_locations DESC;
-------------
CREATE TABLE graph2_state_summary AS
    SELECT
        state,
        COUNT(business_id) AS num_businesses,
        ROUND(AVG(rating), 2) AS avg_rating
    FROM  business_origin
    GROUP BY state
    ORDER BY num_businesses DESC;
-------------
CREATE TABLE graph2_city_summary AS
SELECT 
    LOWER(TRIM(REGEXP_REPLACE(city, '\s+', ' '))) AS city,
    state,
    COUNT(business_id) AS num_businesses,
    ROUND(AVG(rating), 2) AS average_rating
FROM business_origin
WHERE city IS NOT NULL AND city != ''
GROUP BY LOWER(TRIM(REGEXP_REPLACE(city, '\s+', ' '))), state
ORDER BY num_businesses DESC;
-------------
CREATE TABLE graph3_top_rated_cities AS
    SELECT
        city,
        ROUND(AVG(rating), 2) AS avg_rating,
        COUNT(business_id) AS num_businesses
    FROM  business_origin
    WHERE rating IS NOT NULL
    GROUP BY city
    HAVING COUNT(business_id) > 50
    ORDER BY avg_rating DESC;
-------------
CREATE TABLE graph4_cuisine_summary AS
    SELECT
        cuisine_type,
        COUNT(business_id) AS num_businesses
    FROM business_origin
    WHERE cuisine_type IS NOT NULL AND cuisine_type != ''
    GROUP BY cuisine_type
    ORDER BY num_businesses DESC;
-------------
CREATE TABLE graph5_cuisinetip_summary AS
    SELECT
        b.cuisine_type,
        COUNT(DISTINCT b.business_id) AS num_businesses,
        COALESCE(SUM(t.review_count), 0) AS total_reviews
    FROM business_origin AS b
        LEFT JOIN (
        SELECT business_id, COUNT(*) AS review_count
        FROM tip_origin
        GROUP BY business_id
        ) AS t
            ON b.business_id = t.business_id
    WHERE b.cuisine_type IS NOT NULL AND b.cuisine_type != ''
    GROUP BY b.cuisine_type
    ORDER BY num_businesses DESC;
-------------
CREATE TABLE graph6_checkin_aggregates_all AS
      (SELECT
          strftime(check_in_time, '%Y')::INT AS year,
          NULL AS month,
          NULL AS year_month,
          COUNT(*) AS checkin_count
      FROM checkin_origin
      GROUP BY year
      ORDER BY year)
  UNION ALL
      (SELECT
          NULL AS year,
          strftime(check_in_time, '%m')::INT AS month,
          NULL AS year_month,
          COUNT(*) AS checkin_count
      FROM checkin_origin
      GROUP BY month
      ORDER BY month)
  UNION ALL
      (SELECT
          strftime(check_in_time, '%Y')::INT AS year,
          strftime(check_in_time, '%m')::INT AS month,
          strftime(check_in_time, '%Y-%m') AS year_month,
          COUNT(*) AS checkin_count
      FROM checkin_origin
      GROUP BY year_month, year, month
      ORDER BY year_month)
  ;
-----------
create table graph7a_checkin_num_res as (
      select
          b.restaurant_name,
          strftime(check_in_time, '%H:00') as checkin_hour,
          count(*) as checkin_count
      from business_origin b
      join checkin_origin c
      on b.business_id = c.business_id
      group by b.restaurant_name, checkin_hour
      order by b.restaurant_name, checkin_hour
  );
-------------
create table graph7b_checkin_num_all as (
      select
          strftime(check_in_time, '%H:00') as checkin_hour,
          count(*) as checkin_count
      from checkin_origin
      group by checkin_hour
      order by checkin_hour
  );
------------
create table graph8_city_checkin_num as (
      select city, count(check_in_time) as num_checkin
        from business_origin b
        join checkin_origin c
        on b.business_id = c.business_id
        group by b.city
        order by num_checkin desc
  );
-------------
create table graph9_restaurant_checkin_num as (
    select restaurant_name, count(check_in_time) as num_checkin
      from business_origin b
      join checkin_origin c
      on b.business_id = c.business_id
      group by b.restaurant_name
      order by num_checkin desc
  );
-------------
CREATE TABLE graph10_businessmap AS
    SELECT
        business_id,
        state,
        city,
        postal_code,
        latitude,
        longitude,
        restaurant_name
    FROM business_origin
    WHERE state NOT IN ('HI', 'AB', 'XMS');

-- Export results to CSV files for visualization
COPY graph1_business_summary TO '/home/anshadui/team17/duckdb/db_output/graph1_business_summary.csv' (FORMAT CSV, HEADER TRUE);
COPY graph2_city_summary TO '/home/anshadui/team17/duckdb/db_output/graph2_city_summary.csv' (FORMAT CSV, HEADER TRUE);
COPY graph2_state_summary TO '/home/anshadui/team17/duckdb/db_output/graph2_state_summary.csv' (FORMAT CSV, HEADER TRUE);
COPY graph3_top_rated_cities TO '/home/anshadui/team17/duckdb/db_output/graph3_top_rated_cities.csv' (FORMAT CSV, HEADER TRUE);
COPY graph4_cuisine_summary TO '/home/anshadui/team17/duckdb/db_output/graph4_cuisine_summary.csv' (FORMAT CSV, HEADER TRUE);
COPY graph5_cuisinetip_summary TO '/home/anshadui/team17/duckdb/db_output/graph5_cuisinetip_summary.csv' (FORMAT CSV, HEADER TRUE);
COPY graph6_checkin_aggregates_all TO '/home/anshadui/team17/duckdb/db_output/graph6_checkin_aggregates_all.csv' (FORMAT CSV, HEADER TRUE);
COPY graph7a_checkin_num_res TO '/home/anshadui/team17/duckdb/db_output/graph7a_checkin_num_res.csv' (FORMAT CSV, HEADER TRUE);
COPY graph7b_checkin_num_all TO '/home/anshadui/team17/duckdb/db_output/graph7b_checkin_num_all.csv' (FORMAT CSV, HEADER TRUE);
COPY graph8_city_checkin_num TO '/home/anshadui/team17/duckdb/db_output/graph8_city_checkin_num.csv' (FORMAT CSV, HEADER TRUE);
COPY graph9_restaurant_checkin_num TO '/home/anshadui/team17/duckdb/db_output/graph9_restaurant_checkin_num.csv' (FORMAT CSV, HEADER TRUE);
COPY graph10_businessmap TO '/home/anshadui/team17/duckdb/db_output/graph10_businessmap.csv' (FORMAT CSV, HEADER TRUE);

 
