{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='id'
    )
}}

SELECT 
    id,
    email,
    first_name,
    last_name,
    role,
    created_at,
    updated_at
FROM s3(
    'http://minio:9000/staging/incremental/users/*.parquet',  -- ← minio:9000 вместо localhost:9001
    'minioadmin', 
    'minioadmin',
    'Parquet'
)