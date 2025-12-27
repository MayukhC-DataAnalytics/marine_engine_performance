CREATE TABLE staging.raw_engine_data (
    Timestamp TEXT,
    Shaft_RPM NUMERIC,
    Engine_Load NUMERIC,
    Fuel_Flow NUMERIC,
    Air_Pressure NUMERIC,
    Ambient_Temp NUMERIC,
    Oil_Temp NUMERIC,
    Oil_Pressure NUMERIC,
    Vibration_X NUMERIC,
    Vibration_Y NUMERIC,
    Vibration_Z NUMERIC,
    Fault_Label INTEGER,
    Cylinder1_Pressure NUMERIC,
    Cylinder1_Exhaust_Temp NUMERIC,
    Cylinder2_Pressure NUMERIC,
    Cylinder2_Exhaust_Temp NUMERIC,
    Cylinder3_Pressure NUMERIC,
    Cylinder3_Exhaust_Temp NUMERIC,
    Cylinder4_Pressure NUMERIC,
    Cylinder4_Exhaust_Temp NUMERIC
);

-- In a real scenario, you'd use the COPY command:
COPY staging.raw_engine_data FROM 'D:\final_dataset.csv' DELIMITER ',' CSV HEADER;
