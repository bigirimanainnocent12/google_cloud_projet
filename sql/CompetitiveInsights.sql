-- IV/ Analyse concurrentielle et efficacité opérationnelle

--  Question 12 : Quels boroughs ou quelles zones enregistrent les volumes de trajets les plus élevés et les plus faibles, et comment évoluent‑ils au fil du temps ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.trip_volume_by_borough` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    pz.Borough AS pickup_borough,
    dz.Borough AS dropoff_borough,
    pz.Zone AS pickup_zone,
    dz.Zone AS dropoff_zone,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` dz 
    ON t.DOLocationID = dz.LocationID
GROUP BY trip_date, year, month, pickup_borough, dropoff_borough, pickup_zone, dropoff_zone;




-- Question 13 : À quelle fréquence les taxis jaunes desservent‑ils les aéroports (JFK, LaGuardia, …) et quel est le tarif moyen de ces trajets ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.airport_trips_analysis` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    CASE 
        WHEN pz.Zone = 'JFK Airport' OR dz.Zone = 'JFK Airport' THEN 'JFK Airport'
        WHEN pz.Zone = 'LaGuardia Airport' OR dz.Zone = 'LaGuardia Airport' THEN 'LaGuardia Airport'
        WHEN pz.Zone = 'Newark Airport' OR dz.Zone = 'Newark Airport' THEN 'Newark Airport'
        ELSE 'Other'
    END AS airport,
    COUNT(*) AS total_trips,
    ROUND(AVG(t.total_amount), 2) AS avg_fare,
    ROUND(AVG(t.trip_distance), 2) AS avg_distance
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` dz 
    ON t.DOLocationID = dz.LocationID
WHERE pz.Zone IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') 
   OR dz.Zone IN ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
GROUP BY trip_date, year, month, airport;




-- Question 14 : À quelle fréquence les taxis utilisent‑ils les différents codes tarifaires (par exemple, tarif standard vs. tarifs négociés), et comment ces tarifs varient‑ils selon les boroughs ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.rate_code_analysis` AS
SELECT 
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    pz.Borough AS pickup_borough,
    t.RateCodeID,
    CASE 
        WHEN t.RateCodeID = 1 THEN 'Standard rate'
        WHEN t.RateCodeID = 2 THEN 'JFK'
        WHEN t.RateCodeID = 3 THEN 'Newark'
        WHEN t.RateCodeID = 4 THEN 'Nassau or Westchester'
        WHEN t.RateCodeID = 5 THEN 'Negotiated fare'
        WHEN t.RateCodeID = 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_code_description,
    COUNT(*) AS total_trips,
    ROUND(AVG(t.total_amount), 2) AS avg_fare
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
GROUP BY year, month, pickup_borough, t.RateCodeID, rate_code_description;



-- Question 15 : Combien de temps durent généralement les trajets, et observe‑t‑on une tendance à la hausse ou à la baisse de leur durée au fil du temps ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.trip_duration_analysis` AS
SELECT 
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    EXTRACT(DAY FROM t.tpep_pickup_datetime) AS day,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS hour,
    ROUND(AVG(TIMESTAMP_DIFF(t.tpep_dropoff_datetime, t.tpep_pickup_datetime, MINUTE)), 2) AS avg_trip_duration_min,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
GROUP BY year, month, day, hour;
