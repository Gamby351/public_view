--------------------------------------------------------------------------------
-- Description: Vista per la TS in Full advertising_linear_management.sales_product
-- Epic Link: https://agile.at.sky/browse/DPAEP-1508
-- DM: Klodiana Cika
-- PK: ['id_sales_product']
--------------------------------------------------------------------------------

SELECT
    additional_info,
    flg_prohibited_sales_product,
    flg_events_exclusive,
    flg_on_demand,
    id_sales_product_group,
    id_sales_product,
    sales_product_name,
    id_type_stg_sales_product,
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
FROM `$source_project_daita.advertising_linear_management.sales_product`
WHERE id_sales_product IS NOT NULL
AND DATE(partition_date) = DATE((
      SELECT MAX(PARSE_DATE("%Y%m%d",partition_id))
      FROM `$source_project_daita.advertising_linear_management.INFORMATION_SCHEMA.PARTITIONS`
      WHERE table_name="sales_product" AND partition_id!="__NULL__"
    ))
QUALIFY ROW_NUMBER() OVER (PARTITION BY ['id_sales_product'] ORDER BY _process_mapping_start_ts DESC)= 1