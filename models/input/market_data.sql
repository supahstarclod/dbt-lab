SELECT date,
symbol,
close
FROM {{ source('raw_data', 'market_data') }}