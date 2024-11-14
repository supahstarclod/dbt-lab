WITH price_changes AS (
    SELECT 
        symbol, date, close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM {{ ref("market_data") }}
    WHERE symbol = 'TSLA' 
),
gains_losses AS (
    SELECT
        symbol, date, close, prev_close,
        CASE 
            WHEN close > prev_close THEN close - prev_close
            ELSE 0
        END AS gain,
        CASE 
            WHEN close < prev_close THEN prev_close - close
            ELSE 0
        END AS loss
    FROM price_changes
    WHERE prev_close IS NOT NULL 
),
avg_gains_losses AS (
    SELECT 
        symbol, date,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
),
rs_and_rsi AS (
    SELECT 
        symbol, date, avg_gain, avg_loss,
        CASE 
            WHEN avg_loss = 0 THEN NULL 
            ELSE avg_gain / avg_loss
        END AS rs,
        CASE 
            WHEN avg_loss = 0 THEN 100 
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss))) 
        END AS rsi
    FROM avg_gains_losses
)
SELECT 
    symbol, date, rsi
FROM rs_and_rsi
WHERE rsi IS NOT NULL
ORDER BY date DESC