{{ 
    config(
        materialized='table'
    ) 
}}

select 
    a.census_block_code_2020,
    b.census_county_code,
    d.market_name, 
    b.county_name,
    a.state,
    case 
        when d.census_block_code_2020 is not null then 0  -- In Place gets ami_tag = 0
        when c.fips is not null then 1 
        else 0 
    end as ami_tag,
    case 
        when d.census_block_code_2020 is not null then 'In Place'
        else 'Other'
    end as designation
from {{ref('report_predicted_cbs_in_expansion')}} a
left join {{source('report_market','market_crosswalk_latest')}} b
    on trim(a.census_block_code_2020) = trim(b.census_block_code_2020)
left join {{source('project_tmob2514','i3_intrepid_ami_counties')}} c
    on trim(b.census_county_code) = trim(c.fips)
left join {{source('project_tmob2514','report_intrepid_cbs_in_expansion')}} d
    on trim(a.census_block_code_2020) = trim(d.census_block_code_2020)