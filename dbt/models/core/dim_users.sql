{{ config(materialized='table', schema='core') }}

select *
from gh_archive_staging.ref dim_users
