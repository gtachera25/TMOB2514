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
    when owner = 'intrepid' then market
    when region = 'Rhode Island' then market
    else region 
end as market_vac,
geom
from {{ ref('base_i3_and_intrepid_consolidated') }}
where ST_GeometryType(ST_MakeValid(geom)) = 'ST_MultiLineString'