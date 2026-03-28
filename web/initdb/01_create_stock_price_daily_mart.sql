CREATE TABLE IF NOT EXISTS stock_price_daily_mart (
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(100) NOT NULL,
    trade_date DATE NOT NULL,
    open_price BIGINT NOT NULL,
    high_price BIGINT NOT NULL,
    low_price BIGINT NOT NULL,
    close_price BIGINT NOT NULL,
    volume BIGINT NOT NULL,
    price_change_rate DOUBLE PRECISION,
    source TEXT NOT NULL,
    bronze_path TEXT NOT NULL,
    silver_path TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_code, trade_date)
);
