UPSERT_STOCK_PRICE_DAILY_MART_SQL = """
INSERT INTO stock_price_daily_mart (
    stock_code,
    stock_name,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    price_change_rate,
    source,
    bronze_path,
    silver_path,
    collected_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (stock_code, trade_date)
DO UPDATE SET
    stock_name = EXCLUDED.stock_name,
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume,
    price_change_rate = EXCLUDED.price_change_rate,
    source = EXCLUDED.source,
    bronze_path = EXCLUDED.bronze_path,
    silver_path = EXCLUDED.silver_path,
    collected_at = EXCLUDED.collected_at,
    loaded_at = NOW()
"""
