{{ config(materialized='table') }}

select *
from gh_archive_staging.ref dim_org

