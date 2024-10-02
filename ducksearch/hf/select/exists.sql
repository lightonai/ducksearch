SELECT EXISTS(
    SELECT 1
    FROM information_schema.tables
    WHERE
        LOWER(table_name) = LOWER('{table_name}')
        AND table_schema = '{schema}'
) AS table_exists;
