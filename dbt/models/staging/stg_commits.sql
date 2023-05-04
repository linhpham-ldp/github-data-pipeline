{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('gh_archive_staging','raw_data_v3') }}
),
renamed as (
    select id,
           actor_id as user_id,
           repo_id,
           org_id,
        --    JSON_EXTRACT(payload, '$.action') as status,
           created_at,
           FORMAT_DATETIME('%F', created_at) as created_weekdate,
           FORMAT_DATETIME('%a', created_at) as created_weekday,  
           FORMAT_DATETIME('%k', created_at) as created_hour
    from source
    where type = 'PushEvent'
)
select *
from renamed

