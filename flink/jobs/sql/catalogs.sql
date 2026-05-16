-- catalogs.sql: Iceberg REST catalog + namespace setup.
-- Executed once at job startup by every Flink job before any table DDL.

CREATE CATALOG iceberg_cat WITH (
    'type'                  = 'iceberg',
    'catalog-type'          = 'rest',
    'uri'                   = '{iceberg_uri}',
    'warehouse'             = '{iceberg_warehouse}',
    'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'           = '{s3_endpoint}',
    's3.region'             = '{s3_region}',
    's3.path-style-access'  = 'true'
);

CREATE DATABASE IF NOT EXISTS iceberg_cat.bronze;
CREATE DATABASE IF NOT EXISTS iceberg_cat.normalized
