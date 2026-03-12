-- Analyse financière et politique de tarification

-- Question 7 : Comment le revenu total des courses (total fare revenue) des taxis jaunes évolue-t‑il au fil du temps ? 
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.total_fare_revenue_over_time` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    EXTRACT(WEEK FROM t.tpep_pickup_datetime) AS week,
    EXTRACT(DAYOFWEEK FROM t.tpep_pickup_datetime) AS weekday,
    SUM(t.total_amount) AS total_revenue,
    SUM(t.fare_amount) AS fare_revenue,
    SUM(t.tip_amount) AS tip_revenue,
    SUM(t.tolls_amount) AS tolls_revenue,
    SUM(t.congestion_surcharge) AS congestion_revenue
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
GROUP BY trip_date, year, month, week, weekday;



-- Question 8 : Quel est le tarif moyen par course et comment varie‑t‑il selon le borough, le moment de la journée et la distance du trajet ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.avg_fare_analysis` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS pickup_hour,
    pz.Borough AS pickup_borough,
    dz.Borough AS dropoff_borough,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare_per_trip,
    ROUND(AVG(t.total_amount), 2) AS avg_total_amount_per_trip,
    ROUND(AVG(t.trip_distance), 2) AS avg_trip_distance,
    COUNT(*) AS total_trips
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` dz 
    ON t.DOLocationID = dz.LocationID
GROUP BY trip_date, year, month, pickup_hour, pickup_borough, dropoff_borough;




-- Question 9 : Quelle est la proportion des différents types de paiement (carte bancaire, espèces, etc.) et a‑t‑elle évolué au fil du temps ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.payment_type_trends` AS
WITH payment_summary AS (
    SELECT 
        DATE(t.tpep_pickup_datetime) AS trip_date,
        EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
        EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
        EXTRACT(WEEK FROM t.tpep_pickup_datetime) AS week,
        t.payment_type,
        COUNT(*) AS total_trips
    FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
    GROUP BY trip_date, year, month, week, t.payment_type
),
payment_proportion AS (
    SELECT 
        trip_date,
        year,
        month,
        week,
        payment_type,
        total_trips,
        SUM(total_trips) OVER (PARTITION BY trip_date) AS daily_total_trips,
        ROUND(100 * total_trips / SUM(total_trips) OVER (PARTITION BY trip_date), 2) AS payment_percentage
    FROM payment_summary
)
SELECT 
    trip_date,
    year,
    month,
    week,
    CASE 
        WHEN payment_type = 1 THEN 'Credit Card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No Charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
    END AS payment_method,
    total_trips,
    daily_total_trips,
    payment_percentage
FROM payment_proportion;





--  Question 10 : À quelle fréquence les passagers laissent‑ils un pourboire, et quels facteurs (moment de la journée, borough, montant de la course) influencent le montant des pourboires 
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.tipping_behavior_analysis` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime) AS pickup_hour,
    pz.Borough AS pickup_borough,
    dz.Borough AS dropoff_borough,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN t.tip_amount > 0 THEN 1 ELSE 0 END) AS tipped_trips,
    ROUND(100 * SUM(CASE WHEN t.tip_amount > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS tip_frequency_percentage,
    ROUND(AVG(t.tip_amount), 2) AS avg_tip_amount,
    ROUND(AVG(t.total_amount), 2) AS avg_total_fare,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare,
    ROUND(AVG(t.tip_amount / NULLIF(t.total_amount, 0)), 4) * 100 AS avg_tip_percentage
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` pz 
    ON t.PULocationID = pz.LocationID
JOIN `gcp-projet-youtube.rowyellontrips.Zonnes_taxis` dz 
    ON t.DOLocationID = dz.LocationID
WHERE t.payment_type = 1  -- Uniquement les paiements par carte bancaire (les pourboires en espèces ne sont pas enregistrés)
GROUP BY trip_date, year, month, pickup_hour, pickup_borough, dropoff_borough;




-- Question 11 : Quel est le montant des revenus générés par les frais additionnels (taxe MTA, surcharge de congestion, frais d’aéroport) et ont‑ils évolué au fil du temps ?
CREATE OR REPLACE VIEW `gcp-projet-youtube.Wieusrowyellontripsdashboards.additional_charges_revenue` AS
SELECT 
    DATE(t.tpep_pickup_datetime) AS trip_date,
    EXTRACT(YEAR FROM t.tpep_pickup_datetime) AS year,
    EXTRACT(MONTH FROM t.tpep_pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    ROUND(SUM(t.MTA_tax), 2) AS total_MTA_tax,
    ROUND(SUM(t.congestion_surcharge), 2) AS total_congestion_surcharge,
    ROUND(SUM(t.airport_fee), 2) AS total_airport_fees,
    ROUND(SUM(t.MTA_tax + t.congestion_surcharge + t.airport_fee), 2) AS total_additional_revenue,
    ROUND(AVG(t.MTA_tax + t.congestion_surcharge + t.airport_fee), 2) AS avg_additional_charge_per_trip
FROM `gcp-projet-youtube.rowyellontripsclean.transforme_data_trips` t
GROUP BY trip_date, year, month;

