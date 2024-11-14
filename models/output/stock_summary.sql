WITH price_changes AS (
    SELECT 
        symbol, 
        date,
        close,
        -- Calculate the daily price change (difference from previous day)
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM {{ ref("market_data") }}
    WHERE symbol = 'TSLA'  -- Filter by symbol
),
gains_losses AS (
    SELECT
        symbol,
        date,
        close,
        prev_close,
        -- Calculate gain (if positive change) or loss (if negative change)
        CASE 
            WHEN close > prev_close THEN close - prev_close
            ELSE 0
        END AS gain,
        CASE 
            WHEN close < prev_close THEN prev_close - close
            ELSE 0
        END AS loss
    FROM price_changes
    WHERE prev_close IS NOT NULL  -- Exclude the first row (no previous price)
),
avg_gains_losses AS (
    SELECT 
        symbol,
        date,
        -- Calculate the average gain and average loss over the last 14 periods (or adjust for your requirement)
        AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
),
rs_and_rsi AS (
    SELECT 
        symbol,
        date,
        avg_gain,
        avg_loss,
        CASE 
            WHEN avg_loss = 0 THEN NULL  -- Avoid division by zero
            ELSE avg_gain / avg_loss
        END AS rs,
        CASE 
            WHEN avg_loss = 0 THEN 100  -- RSI is 100 if there are no losses
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss))) 
        END AS rsi
    FROM avg_gains_losses
)
SELECT 
    symbol,
    date,
    rsi
FROM rs_and_rsi
WHERE rsi IS NOT NULL  -- Exclude rows where RSI is not calculable (e.g., at the start with insufficient data)
ORDER BY date DESC