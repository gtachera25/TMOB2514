{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)"
        ]
    )
}}
with join_table as(
select a.*, b.geom
 from {{ref('report_cbs_in_footprint')}} a
left join {{ref('base_full_market_cbs')}} b
 on a.census_block_code_2020 = b.census_block_code_2020
)
select 
census_block_code_2020,
case 
    when owner = 'intrepid' then market
    when region = 'Rhode Island' then market
    else region 
end as market_name,
state,
case 
    when owner in ('UC2B', 'i3', 'Consolidated', 'circle fiber','BBN', 'Stratus Networks') then 'WH i3B Bidco LLC/WH i3B Topco, LLC'
    else 'BIF IV Intrepid HoldCo, LLC'
end as provider,
status,
geom
from join_table