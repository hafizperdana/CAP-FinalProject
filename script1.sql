CREATE DATABASE if not exists flight_staging location "/user/hive/warehouse/flight_staging.db";

USE flight_staging;

CREATE EXTERNAL TABLE IF NOT EXISTS fact_flights (
        ICAO24 STRING,
        firstSeen BIGINT,
        estDepartureAirport STRING,
        lastSeen BIGINT,
        estArrivalAirport STRING,
        callsign STRING,
        estDepartureAirportHorizDistance BIGINT,
        estDepartureAirportVertDistance BIGINT,
        estArrivalAirportHorizDistance DOUBLE,
        estArrivalAirportVertDistance DOUBLE,
        departureAirportCandidatesCount BIGINT,
        arrivalAirportCandidatesCount BIGINT
    )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/pablo/final_project/data';

-- CREATE DATABASE if not exists flight_db location "/user/hive/warehouse/flight.db";

-- USE flight_db;

-- CREATE EXTERNAL TABLE IF NOT EXISTS fact_flights (
--         ICAO24 STRING,
--         Callsign STRING,
--         Country STRING,
--         Time_Position BIGINT,
--         Last_Contact BIGINT,
--         Longitude DOUBLE,
--         Latitude DOUBLE,
--         Altitude DOUBLE,
--         On_Ground STRING,
--         Velocity DOUBLE,
--         True_Track DOUBLE,
--         Vertical_Rate DOUBLE,
--         Sensors BIGINT,
--         Geo_Altitude DOUBLE,
--         Squawk STRING,
--         SPI STRING,
--         Position_source BIGINT
--     )
--     STORED AS PARQUET
--     LOCATION 'hdfs://localhost:9000/user/pablo/data/taxi_data'
--     TBLPROPERTIES ('skip.header.line.count'='1');
