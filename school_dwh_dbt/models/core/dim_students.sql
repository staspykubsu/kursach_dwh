{{
    config(
        materialized='incremental',
        engine='VersionedCollapsingMergeTree(sign, version)',
        order_by='student_key',
        unique_key='student_key',
        incremental_strategy='append'
    )
}}

SELECT 
    parseDateTimeBestEffort(trim(BOTH '\n' FROM timestamp_str)) as last_extraction_timestamp
FROM s3(
    'http://minio:9000/staging/metadata/students_last_extraction.txt',
    'minioadmin',
    'minioadmin',
    'CSV',
    'timestamp_str String'
)


SELECT 
    {{ dbt_utils.generate_surrogate_key(['student_id']) }} as student_key,
    student_id as student_id,
    user_id,
    first_name,
    last_name,
    phone_number,
    current_grade,
    user_status,
    version,
    sign
    now() as loaded_at

FROM 