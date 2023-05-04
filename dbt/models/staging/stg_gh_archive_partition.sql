{{ config(
    materialized = 'table',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=['created_at', 'type']
) }}

WITH source AS (
  SELECT *
  FROM `github-data-pipeline.gh_archive_staging.raw_data_v3`
),
renamed AS (
  SELECT 
    id,
    actor_id AS user_id,
    repo_id,
    org_id,
    type,
    JSON_EXTRACT(payload, '$.action') AS status,
    created_at,
    FORMAT_DATETIME('%F', created_at) AS created_weekdate,
    FORMAT_DATETIME('%a', created_at) AS created_weekday,
    FORMAT_DATETIME('%k', created_at) AS created_hour
  FROM source
)
SELECT *
FROM renamed