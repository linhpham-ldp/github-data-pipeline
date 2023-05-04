{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('gh_archive_staging','raw_data_v3') }}
),
renamed as (
    select actor_id as user_id,
           actor_login as user_login,
           actor_gravatar_id as user_gravatar_id,
           actor_url as user_url,
           actor_avatar_url as user_avatar_url
    from source  
)
select distinct *
from renamed

