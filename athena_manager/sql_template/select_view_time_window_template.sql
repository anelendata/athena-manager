SELECT *
FROM {view}
WHERE 1=1
AND from_iso8601_timestamp('{start_at}') <= date_parse(substr("date", 1, 10), '%Y-%m-%d')
AND date_parse(substr("date", 1, 10), '%Y-%m-%d') < from_iso8601_timestamp('{end_at}')
