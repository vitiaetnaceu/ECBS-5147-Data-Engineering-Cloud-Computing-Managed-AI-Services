CREATE EXTERNAL TABLE IF NOT EXISTS raw_views (
  title STRING,
  views BIGINT,
  rank INT,
  date STRING,
  retrieved_at STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://etnav-wikidata/raw-views/';

