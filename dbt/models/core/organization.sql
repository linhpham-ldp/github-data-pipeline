{{ config(materialized='table', schema='core') }}

select
    org_id,
    org_login,
    count(*) as commit_count
from {{ ref('stg_commits') }}
group by org_id, org_login
