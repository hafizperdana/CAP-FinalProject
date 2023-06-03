CREATE DATABASE if not exists flight_staging location "/user/hive/warehouse/flight_staging.db";

USE flight_staging;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_flights (
        ICAO24 STRING,
        first_seen BIGINT,
        est_departure_airport STRING,
        last_seen BIGINT,
        est_arrival_airport STRING,
        callsign STRING,
        est_departure_airport_horiz_distance BIGINT,
        est_departure_airport_vert_distance BIGINT,
        est_arrival_airport_horiz_distance DOUBLE,
        est_arrival_airport_vert_distance DOUBLE,
        departure_airport_candidates_count BIGINT,
        arrival_airport_candidates_count BIGINT
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/final_project/data';
