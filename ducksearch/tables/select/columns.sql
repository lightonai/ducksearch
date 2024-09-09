SELECT column_name
FROM information_schema.columns
WHERE
    lower(table_name) = '{table_name}'
    AND table_schema = '{schema}';
