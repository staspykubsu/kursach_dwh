-- DONE

{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='lesson_pack_key'
    )
}}

WITH all_files AS (
    SELECT DISTINCT
        _path as file_path,
        _file as file_name
    FROM s3(
        'http://minio:9000/staging/full/lesson_packs/*.parquet',
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
    {{ dbt_utils.generate_surrogate_key(['lp.pack_id']) }} as lesson_pack_key,
    lp.pack_id as pack_id,
    lp.lessons_count as lessons_count,
    lp.price as price,
    lp.duration_days as duration_days
FROM s3(
    'http://minio:9000/staging/full/lesson_packs/*.parquet',
    'minioadmin', 
    'minioadmin',
    'Parquet'
) lp
WHERE _file = (SELECT file_name FROM latest_file)