with users as (
    select *
    from {{ ref('dim_users') }}
),
org as (
    select *
    from {{ ref('dim_org') }}
)
select *
from users
limit 10
