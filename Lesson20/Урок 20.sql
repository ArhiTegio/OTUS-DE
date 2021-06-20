Трансформация данных
gcloud dataproc jobs submit hive \
     --cluster hive-cluster \
     --region=${REGION} \
     --execute "INSERT INTO chicago_taxi_trips_parquet
     SELECT
        unique_key,
        taxi_id,
           cast(SUBSTR(trip_start_timestamp, 0, 19) as TIMESTAMP) as trip_start_timestamp,
           cast(SUBSTR(trip_end_timestamp, 0, 19) as TIMESTAMP) as trip_end_timestamp,
           cast(trip_seconds as INT),
           cast(trip_miles as FLOAT),
           cast(pickup_census_tract as INT),
           cast(dropoff_census_tract as INT),
           cast(pickup_community_area as INT),
           cast(dropoff_community_area as INT),
           cast(fare as FLOAT),
           cast(tips as FLOAT),
           cast(tolls as FLOAT),
           cast(extras as FLOAT),
           cast(trip_total as FLOAT),
           payment_type,
           company,
           cast(pickup_latitude as FLOAT),
           cast(pickup_longitude as FLOAT),
           pickup_location,
           cast(dropoff_latitude as FLOAT),
           cast(dropoff_longitude as FLOAT),
           dropoff_location
     FROM chicago_taxi_trips_csv;"
     


1.Вывести динамику количества поездок помесячно
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "
    SELECT month, SUM(trip) as total_trips FROM (SELECT month(trip_start_timestamp) as month, 1 as trip FROM chicago_taxi_trips_parquet) AS t
    GROUP BY month;"

Job [84150cc781924292aad8b5dd8ef8a6ae] submitted.
Waiting for job output...
Connecting to jdbc:hive2://hive-cluster-m:10000
Connected to: Apache Hive (version 2.3.7)
Driver: Hive JDBC (version 2.3.7)
Transaction isolation: TRANSACTION_REPEATABLE_READ
+--------+--------------+
| month  | total_trips  |
+--------+--------------+
| 5      | 798323       |
| 9      | 1030262      |
| 6      | 3490279      |
| 11     | 1262918      |
| 4      | 1079522      |
| 1      | 2848448      |
| 3      | 861664       |
| 8      | 1236524      |
| 10     | 1980097      |
| 12     | 2395786      |
| 2      | 2097876      |
| 7      | 2459646      |
+--------+--------------+
12 rows selected (198.822 seconds)
Beeline version 2.3.7 by Apache Hive
Closing: 0: jdbc:hive2://hive-cluster-m:10000
Job [84150cc781924292aad8b5dd8ef8a6ae] finished successfully.



2.Вывести топ-10 компаний (company) по выручке (trip_total)
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "
    SELECT * FROM(
    SELECT company, SUM(trip_total) as trip_counts FROM chicago_taxi_trips_parquet
    GROUP BY company) AS t
    ORDER BY trip_counts DESC
    LIMIT 10;"

Job [dd41dbc1ae0f47448435d1ab2ac39226] submitted.
Waiting for job output...
Connecting to jdbc:hive2://hive-cluster-m:10000
Connected to: Apache Hive (version 2.3.7)
Driver: Hive JDBC (version 2.3.7)
Transaction isolation: TRANSACTION_REPEATABLE_READ
+----------------------------------+-----------------------+
|            t.company             |     t.trip_counts     |
+----------------------------------+-----------------------+
| Flash Cab                        | 6.734912595323701E7   |
|                                  | 4.01545218550627E7    |
| Taxi Affiliation Services        | 3.836745369476664E7   |
| Yellow Cab                       | 2.459058313706684E7   |
| Chicago Carriage Cab Corp        | 2.3227861778868888E7  |
| Sun Taxi                         | 1.7479736069784977E7  |
| City Service                     | 1.7378260841279954E7  |
| Medallion Leasin                 | 1.6619793629901882E7  |
| Taxi Affiliation Service Yellow  | 1.2578214411636049E7  |
| Dispatch Taxi Affiliation        | 8755131.90277116      |
+----------------------------------+-----------------------+
10 rows selected (54.423 seconds)
Beeline version 2.3.7 by Apache Hive
Closing: 0: jdbc:hive2://hive-cluster-m:10000
Job [dd41dbc1ae0f47448435d1ab2ac39226] finished successfully.




3.Подсчитать долю поездок <5, 5-15, 16-25, 26-100 миль
gcloud dataproc jobs submit hive \
    --cluster hive-cluster \
    --region=${REGION} \
    --execute "
    SELECT miles_range, COUNT(1) as count_miles FROM
    (SELECT
    CASE
	WHEN (trip_miles < 5) THEN '<5'
	WHEN (5 <= trip_miles AND trip_miles <= 15) THEN '5-15'
	WHEN (16 <= trip_miles AND trip_miles <= 25) THEN '16-25'
	WHEN (26 <= trip_miles AND trip_miles <= 100) THEN '26-100'
	ELSE 'not in list range'
    END as miles_range
    FROM chicago_taxi_trips_parquet) AS t
    GROUP BY miles_range;"


Job [e73b84282ebc4a3893785ee0f5cfb5b0] submitted.
Waiting for job output...
Connecting to jdbc:hive2://hive-cluster-m:10000
Connected to: Apache Hive (version 2.3.7)
Driver: Hive JDBC (version 2.3.7)
Transaction isolation: TRANSACTION_REPEATABLE_READ
+--------------------+--------------+
|    miles_range     | count_miles  |
+--------------------+--------------+
| 5-15               | 2449947      |
| 26-100             | 107552       |
| 16-25              | 1289151      |
| <5                 | 17524663     |
| not in list range  | 170032       |
+--------------------+--------------+
5 rows selected (58.352 seconds)
Beeline version 2.3.7 by Apache Hive
Closing: 0: jdbc:hive2://hive-cluster-m:10000
Job [e73b84282ebc4a3893785ee0f5cfb5b0] finished successfully.


