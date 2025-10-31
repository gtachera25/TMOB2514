{{ 
    config(
        materialized='table'
    ) 
}}

select 
a.zipcode,
a.census_block_code_2020,
case 
    when a.market = 'Colorado; BroomfieldCO_boundary' then 'broomfield,co'
    when a.market = 'Colorado; PuebloCO_boundary' then 'pueblo,co'
    when a.market = 'Colorado; NorthglennCO_boundary' then 'northglenn,co'
    when a.market = 'Minnesota; EdenPrairieMN_boundary' then 'edenprairie,mn'
    when a.market = 'Colorado; LouisvilleLafayetteCO_boundary' then 'louisville lafayette,co'
    when a.market = 'Minnesota; BloomingtonMN_boundary' then 'bloomington,mn'
    when a.market = 'Minnesota; StCloudMN_boundary' then 'stcloud,mn'
    when a.market = 'Cherry Creek' then 'cherrycreek,co'
    when a.market = 'Minnesota; WoodburyMN_boundary' then 'woodbury,mn'
    when a.market = 'Minnesota; MinnetonkaMN_boundary' then 'minnetonka,mn'
    when a.market = 'Colorado; SuperiorCO_boundary' then 'superior,co'
    when a.market = 'Thornton, CO' then 'thornton,co'
    when a.market = 'Greenwood Village' then 'greenwood village,co'
    when a.market = 'Colorado; WestminsterExpansionCO_boundary' then 'westminster,co'
    when a.market = 'Colorado; WestminsterCO_boundary' then 'westminster,co'
    when a.market = 'Colorado; LittletonCO_boundary' then 'littleton,co'
    else a.market
end as market_name,
a.unit_buckets,
a.address_count,
b.provider
from {{source('project_tmob2514','existing_footprint_cbs_including_zipcode')}} a
left join {{ref('report_market_cbs')}} b
on a.census_block_code_2020 = b.census_block_code_2020