SHOW catalogs;

SHOW schemas;

CREATE SCHEMA hive.test;

CREATE TABLE hive.test.test_table (
  id INT,
  name VARCHAR
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://raw/warehouse/test_table'
);

INSERT INTO hive.test.test_table VALUES (1, 'test'), (2, 'forge-commerce');

SELECT * FROM hive.test.test_table;
