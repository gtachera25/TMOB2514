{{ 
    config(
        materialized='table'
    ) 
}}

select 
census_block_code_2020,
case 
    when markets = 'Colorado; BroomfieldCO_boundary' then 'broomfield,co'
    when markets = 'Colorado; PuebloCO_boundary' then 'pueblo,co'
    when markets = 'Colorado; NorthglennCO_boundary' then 'northglenn,co'
    when markets = 'Minnesota; EdenPrairieMN_boundary' then 'edenprairie,mn'
    when markets = 'Colorado; LouisvilleLafayetteCO_boundary' then 'louisville lafayette,co'
    when markets = 'Minnesota; BloomingtonMN_boundary' then 'bloomington,mn'
    when markets = 'Minnesota; StCloudMN_boundary' then 'stcloud,mn'
    when markets = 'Cherry Creek' then 'cherrycreek,co'
    when markets = 'Minnesota; WoodburyMN_boundary' then 'woodbury,mn'
    when markets = 'Minnesota; MinnetonkaMN_boundary' then 'minnetonka,mn'
    when markets = 'Colorado; SuperiorCO_boundary' then 'superior,co'
    when markets = 'Thornton, CO' then 'thornton,co'
    when markets = 'Greenwood Village' then 'greenwood village,co'
    when markets = 'Colorado; WestminsterExpansionCO_boundary' then 'westminster,co'
    when markets = 'Colorado; WestminsterCO_boundary' then 'westminster,co'
    when markets = 'Colorado; LittletonCO_boundary' then 'littleton,co'
    when region = 'Rhode Island' then markets
    else region 
end as market_vac,
state,
case when owner in ('UC2B', 'i3', 'Consolidated', 'circle fiber','BBN', 'Stratus Networks') then 'WH i3B Bidco LLC/WH i3B Topco, LLC'
else 'BIF IV Intrepid HoldCo, LLC'
end as provider,
status
from {{source('project_tmob2514','report_cbs_in_footprint')}}