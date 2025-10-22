{{ 
    config(
        materialized='table'
    ) 
}}

with excluding_footprint as(
    select a.*, b.census_block_code_2020 as cb_id from {{source('project_tmob2514','base_cbs')}} a
left join {{source('project_tmob2514','report_cbs_in_footprint')}} b
on a.census_block_code_2020 = b.census_block_code_2020
)
select distinct * from excluding_footprint where cb_id is null