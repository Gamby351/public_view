--------------------------------------------------------------------------------
-- Description: Vista per la TS in Full advertising_linear_management.avail_type
-- Epic Link: https://agile.at.sky/browse/DPAEP-1508
-- DM: Klodiana Cika
-- PK: ['id_avail_type']
--------------------------------------------------------------------------------

SELECT
    des_avail_name,
    cod_avail_position,
    id_avail_type,
    id_content_category,
    cod_external_reference,
    flg_network_spots,
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
FROM `$source_project_daita.advertising_linear_management.avail_type`
WHERE id_avail_type IS NOT NULL
AND DATE(partition_date) = DATE((
      SELECT MAX(PARSE_DATE("%Y%m%d",partition_id))
      FROM `$source_project_daita.advertising_linear_management.INFORMATION_SCHEMA.PARTITIONS`
      WHERE table_name="avail_type" AND partition_id!="__NULL__"
    ))
QUALIFY ROW_NUMBER() OVER (PARTITION BY ['id_avail_type'] ORDER BY _process_mapping_start_ts DESC)= 1