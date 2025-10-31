{{ 
    config(
        materialized='table'
    ) 
}}

select *
from {{source('project_tmob2513', 'report_top_counties_comp_vintage_v900_20')}}