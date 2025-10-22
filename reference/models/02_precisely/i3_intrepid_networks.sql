{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add column network_pkey serial primary key",
            "create index on {{ this }} using gist(geom)",
            "alter table {{ this }}
                alter column geom type geometry(MULTILINESTRING, 4326) 
                USING ST_MakeValid(ST_Force2D(geom)::geometry(MULTILINESTRING, 4326))",
        ]
    )
}}

select 
case when owner in ('UC2B', 'i3', 'Consolidated', 'circle fiber','BBN', 'Stratus Networks') then 'WH i3B Bidco LLC/WH i3B Topco, LLC'
else 'BIF IV Intrepid HoldCo, LLC'
end as provider,
id,
category as infra_type,
status as tag,
case 
    when market = 'Colorado; BroomfieldCO_boundary' then 'broomfield,co'
    when market = 'Colorado; PuebloCO_boundary' then 'pueblo,co'
    when market = 'Colorado; NorthglennCO_boundary' then 'northglenn,co'
    when market = 'Minnesota; EdenPrairieMN_boundary' then 'edenprairie,mn'
    when market = 'Colorado; LouisvilleLafayetteCO_boundary' then 'louisville lafayette,co'
    when market = 'Minnesota; BloomingtonMN_boundary' then 'bloomington,mn'
    when market = 'Minnesota; StCloudMN_boundary' then 'stcloud,mn'
    when market = 'Cherry Creek' then 'cherrycreek,co'
    when market = 'Minnesota; WoodburyMN_boundary' then 'woodbury,mn'
    when market = 'Minnesota; MinnetonkaMN_boundary' then 'minnetonka,mn'
    when market = 'Colorado; SuperiorCO_boundary' then 'superior,co'
    when market = 'Thornton, CO' then 'thornton,co'
    when market = 'Greenwood Village' then 'greenwood village,co'
    when market = 'Colorado; WestminsterExpansionCO_boundary' then 'westminster,co'
    when market = 'Colorado; WestminsterCO_boundary' then 'westminster,co'
    when market = 'Colorado; LittletonCO_boundary' then 'littleton,co'
    when region = 'Rhode Island' then market
    else region 
end as market_vac,
geom
from {{ ref('base_i3_and_intrepid_consolidated') }}
where ST_GeometryType(ST_MakeValid(geom)) = 'ST_MultiLineString'