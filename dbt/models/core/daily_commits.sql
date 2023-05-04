with commits as (
    select *
    from {{ ref('stg_commits') }}
),
users as (
    select *
    from {{ ref('stg_users') }}
),
org as (
    select *
    from {{ ref('stg_org') }}
)
select c.actor_id,
       c.actor_login,
       CASE
            WHEN c.org_id IS NULL THEN 'Individual'
            ELSE c.org_id
       END as c.org_id,
       o.org_login,
       c.created_weekdate,
       c.created_weekday,
       c.created_hour    
from commits c
left join users u on c.user_id = u.user_id
left join org o on c.org_id = o.org_id

