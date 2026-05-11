CREATE DATABASE priceprediction;

DROP SCHEMA IF EXISTS product_catalog CASCADE;
CREATE SCHEMA product_catalog;

CREATE TYPE product_catalog.source_enum AS ENUM (
  'mercari',
  'yahooauction',
  'rakuma'
);

-- CREATE TYPE product_catalog.condition_enum AS ENUM (
--   'New/Unused',
--   'Close to unused',
--   'No obvious damages/dirt',
--   'A little damaged/dirty',
--   'In bad condition overall'
-- );

CREATE TABLE product_catalog.items (
    id            SERIAL      PRIMARY KEY,
    url           TEXT        NOT NULL UNIQUE,
    source        product_catalog.source_enum       NOT NULL,
    name          TEXT       NOT NULL,
    price_yen     NUMERIC(12,2),
    price_vnd     NUMERIC(14,0),
    condition     TEXT,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE product_catalog.prices_history (
    history_id   SERIAL                            PRIMARY KEY,
    item_id      INTEGER REFERENCES product_catalog.items(id) ON DELETE CASCADE,
    record_time  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    price_yen    NUMERIC(12,2),
    price_vnd    NUMERIC(14,0)
);

CREATE OR REPLACE FUNCTION product_catalog.compute_price_vnd()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.price_yen IS NOT NULL THEN
    NEW.price_vnd := ROUND(NEW.price_yen * 180);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_compute_price_vnd
  BEFORE INSERT OR UPDATE ON product_catalog.items
  FOR EACH ROW
  EXECUTE FUNCTION product_catalog.compute_price_vnd();

CREATE OR REPLACE FUNCTION product_catalog.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_set_updated_at
  BEFORE UPDATE ON product_catalog.items
  FOR EACH ROW
  EXECUTE FUNCTION product_catalog.set_updated_at();

CREATE OR REPLACE FUNCTION product_catalog.log_price_change()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.price_yen IS DISTINCT FROM NEW.price_yen
     OR OLD.price_vnd IS DISTINCT FROM NEW.price_vnd THEN
    INSERT INTO product_catalog.prices_history
      (item_id, record_time, price_yen, price_vnd)
    VALUES (OLD.id, NOW(), NEW.price_yen, NEW.price_vnd);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_log_price_change
  AFTER UPDATE ON product_catalog.items
  FOR EACH ROW
  EXECUTE FUNCTION product_catalog.log_price_change();
