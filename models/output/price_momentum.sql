WITH price_momentum AS (
    SELECT 
        symbol, 
        date,
        close,
        LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date) AS close_10_days_ago
    FROM {{ ref("market_data") }}
    WHERE symbol = 'TSLA'
),
momentum_calculation AS (
    SELECT 
        symbol,
        date,
        close,
        close_10_days_ago,
        CASE
            WHEN close_10_days_ago IS NOT NULL THEN 
                ((close - close_10_days_ago) / close_10_days_ago) * 100
            ELSE NULL
        END AS momentum_10_days
    FROM price_momentum
)
SELECT 
    symbol,
    date,
    momentum_10_days
FROM momentum_calculation
WHERE momentum_10_days IS NOT NULL
ORDER BY date DESC