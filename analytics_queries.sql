--Fault Frequency and Impact on Vibration
SELECT 
    df.fault_description,
    COUNT(*) AS occurrence_count,
    ROUND(AVG(f.vibration_x + f.vibration_y + f.vibration_z), 4) AS avg_total_vibration,
    ROUND(AVG(f.oil_temp), 2) AS avg_oil_temp,
    ROUND(AVG(f.oil_pressure), 2) AS avg_oil_pressure
FROM mart.fact_engine_performance f
JOIN mart.dim_fault df ON f.fault_label = df.fault_label
WHERE f.fault_label <> 0 -- Exclude normal operation
GROUP BY df.fault_description
ORDER BY occurrence_count DESC;

/*Average SFOC and Load Factor by Operating Condition This helps the company identify which operating mode (Idle, Cruise, Peak) 
is the most fuel-efficient.*/
SELECT 
    ec.condition_type,
    ROUND(AVG(f.sfoc), 3) AS avg_sfoc,
    ROUND(AVG(f.load_factor), 2) AS avg_load_factor,
    ROUND(SUM(f.fuel_flow), 2) AS total_fuel_consumed
FROM mart.fact_engine_performance f
JOIN mart.dim_engine_condition ec ON f.condition_id = ec.condition_id
GROUP BY ec.condition_type
ORDER BY avg_sfoc ASC;

/* Temperature Hot/ Cold Cylinders*/
WITH AvgExhaust AS (
    SELECT 
        fact_id,
        (cyl1_exhaust_temp + cyl2_exhaust_temp + cyl3_exhaust_temp + cyl4_exhaust_temp) / 4 AS engine_avg_temp
    FROM mart.fact_engine_performance
)
SELECT 
    f.fact_id,
    f.time_id,
    ROUND(ABS(f.cyl1_exhaust_temp - ae.engine_avg_temp), 2) AS cyl1_dev,
    ROUND(ABS(f.cyl2_exhaust_temp - ae.engine_avg_temp), 2) AS cyl2_dev,
    ROUND(ABS(f.cyl3_exhaust_temp - ae.engine_avg_temp), 2) AS cyl3_dev,
    ROUND(ABS(f.cyl4_exhaust_temp - ae.engine_avg_temp), 2) AS cyl4_dev
FROM mart.fact_engine_performance f
JOIN AvgExhaust ae ON f.fact_id = ae.fact_id
WHERE f.fault_label = 7 -- Focus on 'CYLINDER IMBALANCE' fault specifically
LIMIT 20;


/* Anonymous Oil health*/
SELECT 
    dt.timestamp,
    f.engine_load,
    f.oil_pressure,
    f.oil_temp,
    df.fault_description
FROM mart.fact_engine_performance f
JOIN mart.dim_time dt ON f.time_id = dt.time_id
JOIN mart.dim_fault df ON f.fault_label = df.fault_label
WHERE f.oil_pressure < 2.5 -- Threshold for low oil pressure
AND f.engine_load > 70    -- During high stress
ORDER BY dt.timestamp DESC;

/* High Load Vs Fuel Flow*/
SELECT 
    dt.hour,
    ROUND(AVG(f.engine_load), 2) AS avg_load,
    ROUND(AVG(f.sfoc), 3) AS avg_sfoc,
    COUNT(CASE WHEN f.fault_label > 0 THEN 1 END) AS fault_events
FROM mart.fact_engine_performance f
JOIN mart.dim_time dt ON f.time_id = dt.time_id
GROUP BY dt.hour
ORDER BY dt.hour;

/* Exhaust Temp Spread*/
SELECT 
    f.fact_id,
    dt.timestamp,
    (GREATEST(f.cyl1_exhaust_temp, f.cyl2_exhaust_temp, f.cyl3_exhaust_temp, f.cyl4_exhaust_temp) - 
     LEAST(f.cyl1_exhaust_temp, f.cyl2_exhaust_temp, f.cyl3_exhaust_temp, f.cyl4_exhaust_temp)) AS egt_spread,
    f.engine_load
FROM mart.fact_engine_performance f
JOIN mart.dim_time dt ON f.time_id = dt.time_id
WHERE f.engine_load > 50 -- Spread is most relevant under load
ORDER BY egt_spread DESC

/* Efficiency Loss Analysis*/
SELECT 
    df.fault_description,
    ROUND(AVG(f.sfoc), 3) AS avg_sfoc,
    ROUND(AVG(f.sfoc) - (SELECT AVG(sfoc) FROM mart.fact_engine_performance WHERE fault_label = 0), 3) AS efficiency_loss_penalty
FROM mart.fact_engine_performance f
JOIN mart.dim_fault df ON f.fault_label = df.fault_label
GROUP BY df.fault_description
ORDER BY efficiency_loss_penalty DESC;

/* Vibration Trend*/
SELECT 
    dt.timestamp,
    AVG(vibration_x) as avg_vib_x,
    MAX(vibration_x) as peak_vib_x,
    CASE 
        WHEN MAX(vibration_x) > 0.15 THEN 'CRITICAL (Zone D)'
        WHEN MAX(vibration_x) BETWEEN 0.10 AND 0.15 THEN 'ALARM (Zone C)'
        ELSE 'NORMAL'
    END AS vibration_severity_status
FROM mart.fact_engine_performance f
JOIN mart.dim_time dt ON f.time_id = dt.time_id
GROUP BY dt.timestamp
ORDER BY dt.timestamp DESC;


TRUNCATE mart.dim_time CASCADE;

INSERT INTO mart.dim_time (timestamp, date, hour, day_of_week)
SELECT DISTINCT 
    CAST(timestamp AS TIMESTAMP) as ts,
    DATE(CAST(timestamp AS TIMESTAMP)),
    EXTRACT(HOUR FROM CAST(timestamp AS TIMESTAMP)),
    EXTRACT(DOW FROM CAST(timestamp AS TIMESTAMP))
FROM staging.raw_engine_data;

TRUNCATE mart.fact_engine_performance;

INSERT INTO mart.fact_engine_performance (
    time_id, 
    fault_label, 
    condition_id, 
    shaft_rpm, 
    engine_load, 
    fuel_flow, 
    air_pressure,
    ambient_temp, 
    oil_temp, 
    oil_pressure, 
    vibration_x, 
    vibration_y, 
    vibration_z,
    cyl1_pressure, 
    cyl1_exhaust_temp, 
    cyl2_pressure, 
    cyl2_exhaust_temp,
    cyl3_pressure, 
    cyl3_exhaust_temp, 
    cyl4_pressure, 
    cyl4_exhaust_temp
)
SELECT 
    dt.time_id, 
    df.fault_label, 
    dec.condition_id, 
    sed.shaft_rpm, 
    sed.engine_load, 
    sed.fuel_flow,
    sed.air_pressure, 
    sed.ambient_temp, 
    sed.oil_temp, 
    sed.oil_pressure,
    sed.vibration_x, 
    sed.vibration_y, 
    sed.vibration_z,
    sed.cylinder1_pressure, 
    sed.cylinder1_exhaust_temp,
    sed.cylinder2_pressure, 
    sed.cylinder2_exhaust_temp,
    sed.cylinder3_pressure, 
    sed.cylinder3_exhaust_temp,
    sed.cylinder4_pressure, 
    sed.cylinder4_exhaust_temp
FROM staging.raw_engine_data sed
-- Join to Time: Ensure casting matches the dim_time timestamp type
JOIN mart.dim_time dt 
    ON CAST(sed.timestamp AS TIMESTAMP) = dt.timestamp
-- Join to Fault: Direct mapping
JOIN mart.dim_fault df 
    ON sed.fault_label = df.fault_label
-- Join to Condition: Mapping based on defined RPM and Load ranges
JOIN mart.dim_engine_condition dec 
    ON sed.shaft_rpm >= dec.rpm_min 
    AND sed.shaft_rpm < dec.rpm_max 
    AND sed.engine_load >= dec.load_min 
    AND sed.engine_load < dec.load_max;

SELECT *
FROM mart.fact_engine_performance;


