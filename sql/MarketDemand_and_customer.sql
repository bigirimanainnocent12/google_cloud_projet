-- Demande du marché et saisonnalité
-- Question 1 : Comment la demande pour les taxis jaunes fluctue‑t‑elle au fil du temps (quotidiennement, hebdomadairement, mensuellement et selon les saisons) ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.demand_over_time` AS
SELECT 
    DATE(tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
    EXTRACT(WEEK FROM tpep_pickup_datetime) AS week,
    EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) AS weekday,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips`
GROUP BY trip_date, year, month, week, weekday
ORDER BY trip_date;




--  Question 2 : Quelles sont les heures de pointe des courses de taxis jaunes dans les différents boroughs et zones ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.peak_hours_by_zone` AS
SELECT 
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS pickup_hour,
    z.Borough,
    z.Zone,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` z
ON t.PULocationID = z.LocationID
GROUP BY pickup_hour, z.Borough, z.Zone
ORDER BY total_trips DESC;



--  Question 3 : Comment les conditions météorologiques ou les grands événements (jours fériés, événements sportifs) influencent‑ils l’usage des taxis jaunes au fil du temps ? (À analyser ultérieurement)

--  II/ Comportement des clients et caractéristiques des trajets 

-- Question 4 : Quels sont les lieux de prise en charge et de dépose les plus populaires, et comment évoluent‑ils au fil du temps ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.popular_pickup_dropoff` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    EXTRACT(WEEK FROM t.tpep_pickup_datetime) AS week,
    EXTRACT(DAYOFWEEK FROM t.tpep_pickup_datetime) AS weekday,
    pz.Borough AS pickup_borough,
    pz.Zone AS pickup_zone,
    dz.Borough AS dropoff_borough,
    dz.Zone AS dropoff_zone,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` dz 
    ON t.DOLocationID = dz.LocationID
GROUP BY trip_date, year, month, week, weekday, pickup_borough, pickup_zone, dropoff_borough, dropoff_zone;






-- Question 5 : Quelle est la distance moyenne des trajets, et comment varie‑t‑elle selon le borough, le moment de la journée et la saison ? 
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.avg_trip_distance_analysis` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    CASE 
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (9, 10, 11) THEN 'Fall'
    END AS season,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS pickup_hour,
    pz.Borough AS pickup_borough,
    dz.Borough AS dropoff_borough,
    AVG(t.trip_distance) AS avg_trip_distance
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` dz 
    ON t.DOLocationID = dz.LocationID
GROUP BY trip_date, year, month, season, pickup_hour, pickup_borough, dropoff_borough
ORDER BY trip_date, pickup_hour;




-- Question 6 : Combien de trajets comptent un seul passager par rapport à plusieurs passagers, et cette répartition varie‑t‑elle selon les saisons ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.passenger_trends_by_season` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    CASE 
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM t.tpep_pickup_datetime) IN (9, 10, 11) THEN 'Fall'
    END AS season,
    CASE 
        WHEN t.passenger_count = 1 THEN 'Single Passenger'
        ELSE 'Multiple Passengers'
    END AS passenger_category,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
GROUP BY trip_date, year, month, season, passenger_category
ORDER BY trip_date;

