{{ 
    config(
        materialized='table'
    ) 
}}

select 
census_block_code_2020,
case 
    when owner = 'intrepid' then markets
    when region = 'Rhode Island' then markets
    else region 
end as market_vac,
state,
case when owner in ('UC2B', 'i3', 'Consolidated', 'circle fiber','BBN', 'Stratus Networks') then 'WH i3B Bidco LLC/WH i3B Topco, LLC'
else 'BIF IV Intrepid HoldCo, LLC'
end as provider,
status
from {{source('project_tmob2514','report_cbs_in_footprint')}}