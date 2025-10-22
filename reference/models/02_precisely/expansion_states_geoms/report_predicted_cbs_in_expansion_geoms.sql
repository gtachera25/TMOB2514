{{ 
    config(
        materialized='table'
    ) 
}}

with excluding_footprint as(
    select a.*, b.census_block_code_2020 as cb_id from {{ref('base_full_market_cbs')}} a
left join {{ref('report_cbs_in_footprint')}} b
on a.census_block_code_2020 = b.census_block_code_2020
),
final_select as(
select distinct * from excluding_footprint where cb_id is null)
select census_block_code_2020, state, geom 
from final_select