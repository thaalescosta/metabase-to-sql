SELECT 
    'users' as table_name, COUNT(*) as row_count FROM users
UNION ALL
SELECT 
    'deposits' as table_name, COUNT(*) as row_count FROM deposits
UNION ALL
SELECT 
    'trades' as table_name, COUNT(*) as row_count FROM trades;