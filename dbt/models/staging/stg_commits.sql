{{ config(materialized='view', schema='staging') }}

select * from {{ ref('raw_data_v3') }}
where type = 'PushEvent'
