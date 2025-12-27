CREATE SCHEMA mart

CREATE TABLE mart.dim_time(
time_id SERIAL PRIMARY KEY,
timestamp TIMESTAMP NOT NULL, 
date DATE,
hour INT,
day_of_week INT,
is_fault_period BOOLEAN
);

--Populating data to dim_time.
INSERT INTO mart.dim_time (timestamp, 
date, hour, day_of_week, is_fault_period)

SELECT DISTINCT 
    CAST(timestamp AS TIMESTAMP),
    DATE(CAST(timestamp AS TIMESTAMP)),
    EXTRACT(HOUR FROM CAST(timestamp AS TIMESTAMP)),
    EXTRACT(DOW FROM CAST(timestamp AS TIMESTAMP)),
    CASE 
        WHEN fault_label > 0 THEN TRUE 
        ELSE FALSE
    END AS is_fault_period
FROM staging.raw_engine_data;

SELECT *
FROM mart.dim_time
-- END OF DIM_TIME

CREATE TABLE mart.dim_fault(
fault_id SERIAL PRIMARY KEY,
fault_label INT NOT NULL UNIQUE,
fault_description VARCHAR(100)
);
SELECT *
FROM mart.dim_fault
ALTER TABLE mart.dim_fault
RENAME COLUMN fault_level TO fault_label
--Insert Data Into dim_fault
INSERT INTO mart.dim_fault(
fault_label, fault_description)
SELECT DISTINCT fault_label,
CASE fault_label
	WHEN 0 THEN 'NORMAL OPERATION'
	WHEN 1 THEN 'FUEL INJECTION ISSUE'
	WHEN 2 THEN 'LOW SCAVENGE PRESSURE'
	WHEN 3 THEN 'VIBRATION X-AXIS ANAMOLY'
	WHEN 4 THEN 'TURBOCHARGER FAULT'
	WHEN 5 THEN 'OIL PRESSURE DROP'
	WHEN 6 THEN 'HIGH EXHAUST TEMP'
	WHEN 7 THEN 'CYLINDER IMBALANCE'
END
FROM staging.raw_engine_data
ORDER BY fault_label
-- END OF DIM_FAULT

CREATE TABLE  mart.dim_engine_condition(
condition_id SERIAL PRIMARY KEY,
rpm_min DECIMAL(10,2),
rpm_max DECIMAL(10,2),
load_max DECIMAL(5,2),
load_min DECIMAL(5,2),
condition_type VARCHAR(50)
);

SELECT *
FROM mart.dim_engine_condition
INSERT INTO mart.dim_engine_condition(
rpm_min, rpm_max, load_min, load_max, condition_type)
SELECT 
MIN(shaft_rpm) AS rpm_min, MAX(shaft_rpm) AS rpm_max,
MIN(engine_load) AS load_min, MAX(engine_load) AS load_max,
CASE
	WHEN AVG(shaft_rpm) < 900 AND AVG(engine_load) < 60 THEN 'IDLE'
	WHEN AVG(shaft_rpm) BETWEEN 900 AND 1000 AND AVG(engine_load) BETWEEN 60 AND 80 THEN 'NORMAL CRUISE'
	WHEN AVG(shaft_rpm) > 1000 OR AVG(engine_load) > 80 THEN 'HIGH LOAD/PEAK'
	ELSE 'OVERLOAD ALERT TRIGGERED'
END AS condition_type
FROM staging.raw_engine_data

select *
from mart.fact_engine_performance
CREATE TABLE mart.fact_engine_performance(
fact_id BIGSERIAL PRIMARY KEY, 
time_id INT REFERENCES mart.dim_time(time_id),
fault_label INT REFERENCES mart.dim_fault(fault_label),
condition_id INT REFERENCES mart.dim_engine_condition(condition_id),
shaft_rpm DECIMAL(10,2),
engine_load DECIMAL(5,2),
fuel_flow DECIMAL(10,2),--Key KPI: gph ties to SFOC
air_pressure DECIMAL(10,2),
ambient_temp DECIMAL(10,2),
oil_temp DECIMAL(10,2),
oil_pressure DECIMAL(10,2),
vibration_x DECIMAL(10,2), vibration_y DECIMAL(10,4),
vibration_z DECIMAL(10,4),
cyl1_pressure DECIMAL(10,2), cyl1_exhaust_temp DECIMAL(10,2),
cyl2_pressure DECIMAL(10,2), cyl2_exhaust_temp DECIMAL(10,2),
cyl3_pressure DECIMAL(10,2), cyl3_exhaust_temp DECIMAL(10,2),
cyl4_pressure DECIMAL(10,2), cyl4_exhaust_temp DECIMAL(10,2),
sfoc DECIMAL(10,2) GENERATED ALWAYS AS (fuel_flow / NULLIF (engine_load,0))
STORED, --defined efficiency metric
load_factor DECIMAL (5,2) GENERATED ALWAYS AS (engine_load/100) STORED
);

	--ETL
INSERT INTO mart.fact_engine_performance (
    time_id, fault_label, condition_id, shaft_rpm, engine_load, fuel_flow, air_pressure,
    ambient_temp, oil_temp, oil_pressure, vibration_x, vibration_y, vibration_z,
    cyl1_pressure, cyl1_exhaust_temp, cyl2_pressure, cyl2_exhaust_temp,
    cyl3_pressure, cyl3_exhaust_temp, cyl4_pressure, cyl4_exhaust_temp
)
SELECT 
    dt.time_id, df.fault_label, dec.condition_id, sed.shaft_rpm, sed.engine_load, sed.fuel_flow,
    sed.air_pressure, sed.ambient_temp, sed.oil_temp, sed.oil_pressure,
    sed.vibration_x, sed.vibration_y, sed.vibration_z,
    sed.cylinder1_pressure, sed.cylinder1_exhaust_temp,
    sed.cylinder2_pressure, sed.cylinder2_exhaust_temp,
    sed.cylinder3_pressure, sed.cylinder3_exhaust_temp,
    sed.cylinder4_pressure, sed.cylinder4_exhaust_temp
FROM staging.raw_engine_data sed
JOIN mart.dim_time dt ON sed.timestamp = dt.timestamp
JOIN mart.dim_fault df ON sed.fault_label = df.fault_label
JOIN mart.dim_engine_condition dec ON sed.shaft_rpm BETWEEN dec.rpm_min AND dec.rpm_max 
    AND sed.engine_load BETWEEN dec.load_min AND dec.load_max;


SELECT *
FROM mart. fact_engine_performance

ALTER TABLE staging.raw_engine_data 
ALTER COLUMN timestamp TYPE TIMESTAMP WITHOUT TIME ZONE 
USING TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS');

COPY staging.raw_engine_data (
    timestamp, shaft_rpm, engine_load, fuel_flow, air_pressure, 
    ambient_temp, oil_temp, oil_pressure, vibration_x, vibration_y, 
    vibration_z, fault_label, cylinder1_pressure, cylinder1_exhaust_temp,
    cylinder2_pressure, cylinder2_exhaust_temp, cylinder3_pressure, 
    cylinder3_exhaust_temp, cylinder4_pressure, cylinder4_exhaust_temp
)
FROM 'D:\final_dataset.csv' 
DELIMITER ',' 
CSV HEADER 
NULL AS '';
select distinct timestamp
from mart.dim_time
--
SELECT r.shaft_rpm, r.fuel_flow, r.air_pressure, r.oil_temp, t.hour, t.is_fault_period, t.timestamp as time
FROM mart.fact_engine_performance as r
INNER JOIN mart.dim_time as t ON r.time_id = t.time_id
ORDER BY t.timestamp DESC  -- **FIX: Use table.column instead of alias**
LIMIT 100;

