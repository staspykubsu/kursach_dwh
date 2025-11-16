SELECT 
    _file as file_name
FROM s3(
    'http://minio:9000/staging/full/lesson_packs/*.parquet',
    'minioadmin', 
    'minioadmin',
    'Parquet'
)
ORDER BY _file DESC
LIMIT 1