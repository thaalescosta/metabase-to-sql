-- Winners Table
WITH user_deposits AS (
    SELECT 
        user_id,
        COUNT(transaction_id) as deposit_count
    FROM deposits
    WHERE transaction_type = 'deposit'
    GROUP BY user_id
    HAVING COUNT(transaction_id) > 1
),
user_pnl AS (
    SELECT 
        t.user_id,
        MAX(t.position_created) as last_trade_date,
        SUM(t.gross_pnl) as net_pnl_real
    FROM trades t
    GROUP BY t.user_id
)

SELECT 
    u.user_id,
    u.email,
    ud.deposit_count,
    up.net_pnl_real,
    CASE 
        WHEN up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 'Ganhadores 7 Dias'
        WHEN up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 'Ganhadores 15 Dias'
        WHEN up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 'Ganhadores 30 Dias'
    END as segmentation
FROM users u
JOIN user_deposits ud ON u.user_id = ud.user_id
JOIN user_pnl up ON u.user_id = up.user_id
WHERE up.net_pnl_real > 0
AND up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
AND NOT EXISTS (
    -- Exclude users who appear in the losers table
    SELECT 1 
    FROM user_pnl up2 
    WHERE up2.user_id = u.user_id 
    AND up2.net_pnl_real < 0
    AND up2.last_trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
);

-- Losers Table
WITH user_deposits AS (
    SELECT 
        user_id,
        COUNT(transaction_id) as deposit_count
    FROM deposits
    WHERE transaction_type = 'deposit'
    GROUP BY user_id
    HAVING COUNT(transaction_id) > 1
),
user_pnl AS (
    SELECT 
        t.user_id,
        MAX(t.position_created) as last_trade_date,
        SUM(t.gross_pnl) as net_pnl_real
    FROM trades t
    GROUP BY t.user_id
)

SELECT 
    u.user_id,
    u.email,
    ud.deposit_count,
    up.net_pnl_real,
    CASE 
        WHEN up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 'Perdedores 7 Dias'
        WHEN up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 15 DAY) THEN 'Perdedores 15 Dias'
        WHEN up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 'Perdedores 30 Dias'
    END as segmentation
FROM users u
JOIN user_deposits ud ON u.user_id = ud.user_id
JOIN user_pnl up ON u.user_id = up.user_id
WHERE up.net_pnl_real < 0
AND up.last_trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
AND NOT EXISTS (
    -- Exclude users who appear in the winners table
    SELECT 1 
    FROM user_pnl up2 
    WHERE up2.user_id = u.user_id 
    AND up2.net_pnl_real > 0
    AND up2.last_trade_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
);

-- New Users Table
WITH user_deposits AS (
    SELECT 
        user_id,
        COUNT(transaction_id) as deposit_count
    FROM deposits
    WHERE transaction_type = 'deposit'
    GROUP BY user_id
),
user_training_balance AS (
    SELECT 
        user_id,
        SUM(CASE WHEN instrument_type = 'demo' THEN gross_pnl ELSE 0 END) as training_balance
    FROM trades
    GROUP BY user_id
)

SELECT 
    u.user_id,
    u.email,
    u.registered,
    CASE 
        WHEN u.registered >= DATE_SUB(NOW(), INTERVAL 48 HOUR) THEN 'Novo - Últimas 24h'
        WHEN utb.training_balance > 10000 AND (ud.deposit_count = 0 OR ud.deposit_count IS NULL) THEN 'Novo - Ganhando na Demo'
        WHEN utb.training_balance < 10000 AND (ud.deposit_count = 0 OR ud.deposit_count IS NULL) THEN 'Novo - Perdendo na Demo'
    END as segmentation,
    utb.training_balance
FROM users u
LEFT JOIN user_deposits ud ON u.user_id = ud.user_id
LEFT JOIN user_training_balance utb ON u.user_id = utb.user_id
WHERE 
    (u.registered >= DATE_SUB(NOW(), INTERVAL 48 HOUR))
    OR 
    (u.registered >= DATE_SUB(NOW(), INTERVAL 6 DAY) 
     AND (ud.deposit_count = 0 OR ud.deposit_count IS NULL)
     AND utb.training_balance IS NOT NULL); 