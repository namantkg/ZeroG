

CREATE DATABASE OPEN_SKY_DB;

-----------------------

CREATE SCHEMA opensky;

------------------------

CREATE OR REPLACE STAGE file_stage
  file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1);

------------------------

CREATE OR REPLACE TABLE stage_opensky_state
(
record_time     string
,baro_altitude	string
,callsign		string
,geo_altitude	string
,heading		string
,icao24			string
,last_contact	string	
,latitude		string
,longitude		string
,on_ground		string
,origin_country	string
,position_source string
,sensors		string
,spi			string
,squawk			string
,time_position	string	
,velocity		string
,vertical_rate	string
,run_key        string
);

---------------------

CREATE OR REPLACE TABLE opensky_state
(
record_time		timestamp
,baro_altitude	float
,callsign		string
,geo_altitude	float
,heading		float
,icao24			string
,last_contact	timestamp	
,latitude		float
,longitude		float
,on_ground		boolean
,origin_country	string
,position_source int
,sensors		string
,spi			boolean
,squawk			string
,time_position	timestamp	
,velocity		float
,vertical_rate	float
,run_key        int  
);

----------------------

DROP SEQUENCE IF EXISTS seq_run_key;
CREATE OR REPLACE SEQUENCE seq_run_key START = 1 INCREMENT=1;

CREATE OR REPLACE TABLE audit_table(
 run_key int DEFAULT seq_run_key.nextval,
 start_time timestamp_ltz,
 end_time timestamp_ltz,
 run_status string,
 file_name string
);

------------------------


CREATE OR REPLACE PROCEDURE sp_reporting_table_load(database_name VARCHAR, schema_name VARCHAR, source_table VARCHAR, target_table VARCHAR)
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
AS
$$

stmt1 = snowflake.createStatement({sqlText: "INSERT INTO "+ DATABASE_NAME +"." + SCHEMA_NAME +"."+ TARGET_TABLE +" ( \
record_time, \
baro_altitude, \
callsign, \
geo_altitude, \
heading, \
icao24, \
last_contact, \
latitude, \
longitude, \
on_ground, \
origin_country, \
position_source, \
sensors, \
spi, \
squawk, \
time_position, \
velocity, \
vertical_rate, \
run_key \
) \
SELECT \
CASE WHEN record_time = 'None' THEN NULL ELSE TO_TIMESTAMP(record_time) END AS record_time \
,CASE WHEN baro_altitude = 'None' THEN NULL ELSE TO_NUMBER(baro_altitude,10,2) END AS baro_altitude \
,CASE WHEN callsign = 'None' THEN NULL ELSE callsign END AS callsign \
,CASE WHEN geo_altitude = 'None' THEN NULL ELSE TO_NUMBER(geo_altitude,10,2) END AS geo_altitude \
,CASE WHEN heading = 'None' THEN NULL ELSE TO_NUMBER(heading,10,3) END AS heading \
,CASE WHEN icao24 = 'None' THEN NULL ELSE icao24 END AS icao24 \
,CASE WHEN last_contact = 'None' THEN NULL ELSE TO_TIMESTAMP(last_contact) END AS last_contact \
,CASE WHEN latitude = 'None' THEN NULL ELSE TO_NUMBER(latitude,10,4) END AS latitude \
,CASE WHEN longitude = 'None' THEN NULL ELSE TO_NUMBER(longitude,10,4) END AS longitude \
,CASE WHEN on_ground = 'None' THEN NULL ELSE TO_BOOLEAN(on_ground) END AS on_ground \
,CASE WHEN origin_country = 'None' THEN NULL ELSE origin_country END AS origin_country \
,CASE WHEN position_source = 'None' THEN NULL ELSE TO_NUMBER(position_source) END AS position_source \
,CASE WHEN sensors = 'None' THEN NULL ELSE sensors END AS sensors \
,CASE WHEN spi = 'None' THEN NULL ELSE TO_BOOLEAN(spi) END AS spi \
,CASE WHEN squawk = 'None' THEN NULL ELSE squawk END AS squawk \
,CASE WHEN time_position = 'None' THEN NULL ELSE TO_TIMESTAMP(time_position) END AS time_position \
,CASE WHEN velocity = 'None' THEN NULL ELSE TO_NUMBER(velocity,10,2) END AS velocity \
,CASE WHEN vertical_rate = 'None' THEN NULL ELSE TO_NUMBER(vertical_rate,10,2) END AS vertical_rate \
,run_key AS run_key \
FROM "+ DATABASE_NAME +"." + SCHEMA_NAME +"."+ SOURCE_TABLE});

rs = stmt1.execute();
rs.next();

output = rs.getColumnValue(1);
return output;
$$
;

---------------------------------------------------------------

CALL sp_reporting_table_load('OPEN_SKY_DB', 'OPENSKY', 'STAGE_OPENSKY_STATE', 'OPENSKY_STATE');

---------------------------------------------------------------

-- Data Insight Queries


--1. Total number of flights in air all over the world during 2AM IST to 3 AM IST

SELECT COUNT(DISTINCT icao24) AS Number_of_Flights
FROM open_sky_db.opensky.opensky_state
WHERE on_ground = FALSE;

----------------------------------------------------

--2. Top 5 countries having high air traffic

SELECT origin_country AS country, flight_count AS number_of_flights
FROM
(SELECT origin_country, COUNT(DISTINCT icao24) AS flight_count
FROM open_sky_db.opensky.opensky_state
WHERE on_ground = FALSE
GROUP BY origin_country)
ORDER BY flight_count desc LIMIT 5;

--------------------------------------------------

-- 3. Top 5 flights moving with high velocity.

SELECT icao24, max_velocity
FROM
(SELECT icao24, MAX(velocity) AS max_velocity 
FROM open_sky_db.opensky.opensky_state
WHERE velocity IS NOT NULL
GROUP BY icao24
)
ORDER BY max_velocity DESC
LIMIT 5;

-------------------------------------------

-- 4. Count of flights on ground vs count of flight in space at particular time

SELECT CASE WHEN on_ground = TRUE THEN 'Ground' ELSE 'Air' END AS Position, Number_of_Flights
FROM
(
SELECT on_ground, COUNT(DISTINCT icao24) AS Number_of_Flights
FROM open_sky_db.opensky.opensky_state
GROUP BY on_ground
);

-- 5. Detailed Report where Difference between geo_altitude and baro_altitude readings is more than 1000 points

SELECT icao24, origin_country , latitude, longitude, geo_altitude, baro_altitude, (geo_altitude-baro_altitude) AS Difference
FROM open_sky_db.opensky.opensky_state
WHERE Difference > 1000;









