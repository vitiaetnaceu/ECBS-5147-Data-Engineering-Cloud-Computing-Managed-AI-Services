CREATE OR REPLACE VIEW views AS
SELECT
  title,
  views,
  rank,
  date
FROM raw_views
ORDER BY date DESC, rank ASC;
