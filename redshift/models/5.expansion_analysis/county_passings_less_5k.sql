{{ 
    config(
        materialized='table'
    ) 
}}

with county_passings as(
select county, sum(total_passings_in_footprint) from project_tmob2514.report_predicted_expansion_tract_demos_for_tagging
group by 1)
select county,
case
    when sum < 5000 then 1
    else 0
end as county_passings_less_5k_flag
from county_passings
