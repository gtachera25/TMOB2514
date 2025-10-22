{{ 
    config(
        materialized='table'
    ) 
}}

with final_select as(
select b.census_tract_code,a.census_block_code_2020, a.county_name, a.state, a.ami_tag
from {{ref('report_predicted_cbs_in_expansion_ami_tagged')}} a
left join {{source('report_market','market_crosswalk_latest')}} b
on a.census_block_code_2020 = b.census_block_code_2020
group by 1,2,3,4,5)
select * from final_select