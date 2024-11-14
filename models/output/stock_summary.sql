WITH rsi AS (
    SELECT date, rsi FROM {{ ref("rsi") }}
), 
ma AS (
    SELECT date, moving_avg_7_days FROM {{ ref("moving_avg_7d") }}
),
momentum AS (
    SELECT date, momentum_10_days FROM {{ ref("price_momentum") }}
)
SELECT t1.date, t1.rsi, t2.moving_avg_7_days, t3.momentum_10_days
FROM rsi t1
JOIN moving_avg_7d t2 ON t1.date = t2.date
JOIN price_momentum t3 ON t1.date = t3.date
ORDER BY t1.date 