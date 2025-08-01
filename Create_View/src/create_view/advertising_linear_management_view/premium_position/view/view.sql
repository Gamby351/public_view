--------------------------------------------------------------------------------
-- Description: Vista per la TS in Full advertising_linear_management.premium_position
-- Epic Link: https://agile.at.sky/browse/DPAEP-1508
-- DM: Klodiana Cika
-- PK: id_premium_position
--------------------------------------------------------------------------------

SELECT
    break_position,
    des_premium_position,
    id_premium_position,
    partition_date,
    _process_mapping_start_ts,
    _process_transaction_id,
    _process_mapping_version,
    _clustering_hour,
    _clustering_minute,
    _ingestion_worker_ts,
    _ingestion_ingester_read_ts,
    _ingestion_ingester_write_ts,
    _ingest_row_id,
    _ingest_schema_version,
    _ingestion_file_name,
FROM `$source_project_daita.advertising_linear_management.premium_position`
WHERE id_premium_position IS NOT NULL
AND DATE(partition_date) = DATE((
      SELECT MAX(PARSE_DATE("%Y%m%d",partition_id))
      FROM `$source_project_daita.advertising_linear_management.INFORMATION_SCHEMA.PARTITIONS`
      WHERE table_name="premium_position" AND partition_id!="__NULL__"
    ))
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_premium_position ORDER BY _process_mapping_start_ts DESC)= 1