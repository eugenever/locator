DROP TABLE IF EXISTS report;
CREATE TABLE report (
    id BIGSERIAL NOT NULL,
    submitted_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_error TEXT,

    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    
    user_agent TEXT,
    raw BYTEA NOT NULL,

    PRIMARY KEY(id, submitted_at)
) PARTITION BY RANGE(submitted_at);

-- CREATE INDEX report_todo ON report (submitted_at) WHERE processed_at IS NULL;
-- CREATE INDEX report_processed_at ON report (processed_at);
-- CREATE INDEX report_error ON report (id) WHERE processing_error IS NOT NULL;

-- пример использования:
-- select create_daily_partitions('report', date '2025-09-01', 31);
DROP FUNCTION IF EXISTS create_daily_partitions;
CREATE FUNCTION create_daily_partitions(
  parent_table TEXT,        -- имя родителя, например 'document'
  start_date   DATE,        -- с какой даты создавать
  days         INT          -- сколько дней вперед
) RETURNS VOID
LANGUAGE PLPGSQL
AS $$
DECLARE
  d DATE;
  sql TEXT;
  part_name TEXT;
BEGIN
  FOR i IN 0..days-1 LOOP
    d := start_date + i;
    part_name := FORMAT('%I_%s', parent_table, TO_CHAR(d, 'YYYY_MM_DD'));
    sql := FORMAT(
      'CREATE TABLE IF NOT EXISTS %s partition OF %I
         FOR VALUES FROM (%L) TO (%L);',
      part_name, parent_table, d::TIMESTAMPTZ, (d + 1)::TIMESTAMPTZ
    );
    EXECUTE sql;
  END LOOP;
END
$$;

-- пример: держим 120 дней, без CASCADE
-- select drop_old_partitions('report', 120, false);
DROP FUNCTION IF EXISTS drop_old_partitions;
CREATE FUNCTION drop_old_partitions(
  table_prefix TEXT,   -- префикс родителя, например 'document'
  keep_days    INT,    -- сколько дней держать
  use_cascade  BOOLEAN DEFAULT false -- использовать ли CASCADE (по умолчанию нет)
) RETURNS VOID
LANGUAGE PLPGSQL
AS $$
DECLARE
  r record;
  cutoff DATE := (NOW() - (keep_days || ' days')::interval)::DATE;
  part_date DATE;
  drop_sql TEXT;
BEGIN
  FOR r IN
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE tablename LIKE table_prefix || '\_%' escape '\'
  LOOP
    BEGIN
      part_date := to_date(
        SUBSTRING(r.tablename FROM '.+_(\d{4}_\d{2}_\d{2})$'),
        'YYYY_MM_DD'
      );

      IF part_date IS NOT NULL AND part_date < cutoff THEN
        drop_sql := FORMAT('DROP TABLE IF EXISTS %I.%I %s;',
          r.schemaname, r.tablename,
          CASE WHEN use_cascade THEN 'cascade' ELSE '' END
        );
        EXECUTE drop_sql;
      END IF;

    exception WHEN others THEN
      raise notice 'skip %.% due to: %', r.schemaname, r.tablename, sqlerrm;
    END;
  END LOOP;
END
$$;

-- пример:
-- select ensure_hot_indexes('report_2025_08_21');
DROP FUNCTION IF EXISTS ensure_hot_indexes;
CREATE FUNCTION ensure_hot_indexes(part_table text)
RETURNS VOID
LANGUAGE PLPGSQL
AS $$
BEGIN
  EXECUTE FORMAT(
    'CREATE INDEX IF NOT EXISTS %I_processed_submitted_idx
       on %I (processed_at, submitted_at)
     WHERE processed_at IS NULL;',
    part_table || '_', part_table
  );

  EXECUTE FORMAT(
    'CREATE INDEX IF NOT EXISTS %I_submitted_idx
       on %I (submitted_at);',
    part_table || '_', part_table
  );
end $$;

-- select ensure_brin_created('report_2025_01');
DROP FUNCTION IF EXISTS ensure_brin_created;
CREATE FUNCTION ensure_brin_created(part_table TEXT)
RETURNS VOID
LANGUAGE PLPGSQL
AS $$
BEGIN
  EXECUTE FORMAT(
    'CREATE INDEX IF NOT EXISTS %I_submitted_brin
       on %I USING BRIN (submitted_at) WITH (pages_per_range=128);',
    part_table || '_', part_table
  );
end $$;

DROP TABLE IF EXISTS cell;
CREATE TABLE cell (
    radio SMALLINT NOT NULL,
    country SMALLINT NOT NULL,
    network SMALLINT NOT NULL,
    area INTEGER NOT NULL,
    cell BIGINT NOT NULL,
    unit SMALLINT NOT NULL,
    PRIMARY KEY (radio, country, network, area, cell, unit),

    min_lat DOUBLE PRECISION NOT NULL,
    min_lon DOUBLE PRECISION NOT NULL,
    max_lat DOUBLE PRECISION NOT NULL,
    max_lon DOUBLE PRECISION NOT NULL,

    -- to use RSSI
    lat DOUBLE PRECISION NOT NULL DEFAULT 0,
    lon DOUBLE PRECISION NOT NULL DEFAULT 0,
    accuracy DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_weight DOUBLE PRECISION NOT NULL DEFAULT 0,

    min_strength SMALLINT NOT NULL,
    max_strength SMALLINT NOT NULL
) WITH (
    fillfactor = 70,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_scale_factor = 0.01
);

DROP TABLE IF EXISTS wifi;
CREATE TABLE wifi (
    mac VARCHAR(128) NOT NULL PRIMARY KEY,

    min_lat DOUBLE PRECISION NOT NULL,
    min_lon DOUBLE PRECISION NOT NULL,
    max_lat DOUBLE PRECISION NOT NULL,
    max_lon DOUBLE PRECISION NOT NULL,

    -- to use RSSI
    lat DOUBLE PRECISION NOT NULL DEFAULT 0,
    lon DOUBLE PRECISION NOT NULL DEFAULT 0,
    accuracy DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_weight DOUBLE PRECISION NOT NULL DEFAULT 0,

    min_strength SMALLINT NOT NULL,
    max_strength SMALLINT NOT NULL
) WITH (
    fillfactor = 70,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_scale_factor = 0.01
);

DROP TABLE IF EXISTS bluetooth;
CREATE TABLE bluetooth (
    mac VARCHAR(128) NOT NULL PRIMARY KEY,

    min_lat DOUBLE PRECISION NOT NULL,
    min_lon DOUBLE PRECISION NOT NULL,
    max_lat DOUBLE PRECISION NOT NULL,
    max_lon DOUBLE PRECISION NOT NULL,

    -- to use RSSI
    lat DOUBLE PRECISION NOT NULL DEFAULT 0,
    lon DOUBLE PRECISION NOT NULL DEFAULT 0,
    accuracy DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_weight DOUBLE PRECISION NOT NULL DEFAULT 0,

    min_strength SMALLINT NOT NULL,
    max_strength SMALLINT NOT NULL
) WITH (
    fillfactor = 70,
    autovacuum_vacuum_threshold = 100,
    autovacuum_vacuum_scale_factor = 0.01
);

DROP TABLE IF EXISTS mls_cell;
CREATE TABLE mls_cell (
    radio SMALLINT NOT NULL,
    country SMALLINT NOT NULL,
    network SMALLINT NOT NULL,
    area INTEGER NOT NULL,
    cell BIGINT NOT NULL,
    unit SMALLINT NOT NULL,
    PRIMARY KEY (radio, country, network, area, cell, unit),

    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    radius DOUBLE PRECISION NOT NULL
);
