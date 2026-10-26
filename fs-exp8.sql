CREATE DATABASE kafka_reliability;
USE kafka_reliability;
SELECT*FROM orders;
TRUNCATE TABLE orders;

SELECT*FROM processed_events;
TRUNCATE TABLE processed_events;

SELECT*FROM event_tracking;
TRUNCATE TABLE event_tracking;