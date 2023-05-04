{{ config(materialized='view') }}

select * from {{ source('gh_archive_staging','raw_data_v3') }}
where type = 'PushEvent'
