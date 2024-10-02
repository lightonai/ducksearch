SELECT coalesce(EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE
        table_name = 'settings'
        AND table_schema = '{schema}'
), FALSE) AS table_exists;
