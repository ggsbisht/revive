create schema if not exists ingest_schema;

CREATE OR REPLACE TABLE ingest_schema.customer_raw (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING
);

INSERT INTO ingest_schema.customer_raw VALUES
(1,'John Doe','john@test.com','New York'),
(2,'Jane Smith','jane@test.com','London'),
(3,'Mike Ross','mike@test.com','Toronto');