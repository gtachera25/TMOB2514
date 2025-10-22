{{
    config(
        materialized='table',
        dist='census_block_code_2020',
        sort=['census_block_code_2020']
    )
}}
select 
census_block_code_2020,
case 
    when market_name = 'Colorado; BroomfieldCO_boundary' then 'broomfield,co'
    when market_name = 'Colorado; PuebloCO_boundary' then 'pueblo,co'
    when market_name = 'Colorado; NorthglennCO_boundary' then 'northglenn,co'
    when market_name = 'Minnesota; EdenPrairieMN_boundary' then 'edenprairie,mn'
    when market_name = 'Colorado; LouisvilleLafayetteCO_boundary' then 'louisville lafayette,co'
    when market_name = 'Minnesota; BloomingtonMN_boundary' then 'bloomington,mn'
    when market_name = 'Minnesota; StCloudMN_boundary' then 'stcloud,mn'
    when market_name = 'Cherry Creek' then 'cherrycreek,co'
    when market_name = 'Minnesota; WoodburyMN_boundary' then 'woodbury,mn'
    when market_name = 'Minnesota; MinnetonkaMN_boundary' then 'minnetonka,mn'
    when market_name = 'Colorado; SuperiorCO_boundary' then 'superior,co'
    when market_name = 'Thornton, CO' then 'thornton,co'
    when market_name = 'Greenwood Village' then 'greenwood village,co'
    when market_name = 'Colorado; WestminsterExpansionCO_boundary' then 'westminster,co'
    when market_name = 'Colorado; WestminsterCO_boundary' then 'westminster,co'
    when market_name = 'Colorado; LittletonCO_boundary' then 'littleton,co'
    else market_name
end as markets,
provider,
unit_buckets,
band_label,
address_count
from {{source('project_tmob2514','i3_intrepid_cb_address_view_residential')}}