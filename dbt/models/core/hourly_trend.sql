{{ config(
    materialized = 'table',
) }}

WITH gh_data AS (
    SELECT *
    FROM {{ ref('stg_gh_archive_partition') }}
),
-- users as (
--     select *
--     from {{ ref('stg_users') }}
-- ),
org AS (
    SELECT *
    FROM {{ ref('stg_org') }}
)
SELECT 
       o.org_login,
       created_hour,
       type,
       count(distinct id) AS event_count  
FROM gh_data gh
-- left join users u on gh.user_id = u.user_id
left join org o on gh.org_id = o.org_id
GROUP BY 1,2,3

