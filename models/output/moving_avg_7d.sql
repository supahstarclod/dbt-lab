SELECT 
    symbol, date, 
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7_days
FROM {{ ref("market_data") }}
WHERE symbol = 'TSLA'
ORDER BY date DESC