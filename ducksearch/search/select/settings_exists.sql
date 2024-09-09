SELECT CASE
    WHEN EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'settings'
        AND table_schema = '{schema}'
    )
    THEN TRUE
    ELSE FALSE
END AS table_exists;
