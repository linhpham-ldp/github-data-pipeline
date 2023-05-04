{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('gh_archive_staging','raw_data_v3') }}
),
renamed as (
    select org_id,
           org_login,
           org_gravatar_id,
           org_url as user_url,
           org_avatar_url
    from source
)
select distinct *
from renamed
where org_id is not null