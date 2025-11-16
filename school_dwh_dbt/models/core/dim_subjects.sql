-- DONE

{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='subject_key'
    )
}}

WITH all_files AS (
    SELECT DISTINCT
        _path as file_path,
        _file as file_name
    FROM s3(
        'http://minio:9000/staging/full/subjects/*.parquet',
        'minioadmin', 
        'minioadmin',
        'Parquet'
    )
),
latest_file AS (
    SELECT file_name
    FROM all_files
    ORDER BY file_name DESC
    LIMIT 1
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['s.subject_id']) }} as subject_key,
    s.subject_id as subject_id,
    s.name as name

FROM s3(
    'http://minio:9000/staging/full/subjects/*.parquet',
    'minioadmin', 
    'minioadmin',
    'Parquet'
) s
WHERE _file = (SELECT file_name FROM latest_file)