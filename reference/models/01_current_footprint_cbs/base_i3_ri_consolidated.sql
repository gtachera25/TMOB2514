{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime = 0.18 seconds, 21165 rows
with consolidated as (
select 
ogc_fid as id,
null as cable_or_fiber_id,
'Barrington, RI' as market,
'Rhode Island' as region,
'i3' as owner,
null as status,
case 
    when folders like '%COAX CABLE%' then 'cable'
    when folders like '%FIBER OPTIC CABLE%' then 'fiber'
    when folders like '%CONDUIT%' then 'conduit'
    else 'other'
end as category,
length as cable_or_fiber_length,
geom
from {{source('tmob2514', 'source_i3_barrington_cable_20251006')}}
union all
select 
ogc_fid as id,
null as cable_or_fiber_id,
'Bristol, RI' as market,
'Rhode Island' as region,
'i3' as owner,
null as status,
case 
    when folders like '%COAX CABLE%' then 'cable'
    when folders like '%FIBER OPTIC CABLE%' then 'fiber'
    when folders like '%CONDUIT%' then 'conduit'
    else 'other'
end as category,
length as cable_or_fiber_length,
geom
from {{source('tmob2514', 'source_i3_bristol_cable_20251006')}}
union all
select 
id,
null as cable_or_fiber_id,
'Warren, RI' as market,
'Rhode Island' as region,
'i3' as owner,
null as status,
case 
    when folders like '%COAX CABLE%' then 'cable'
    when folders like '%FIBER OPTIC CABLE%' then 'fiber'
    when folders like '%CONDUIT%' then 'conduit'
    else 'other'
end as category,
null as cable_or_fiber_length,
geom
from {{source('tmob2514', 'source_i3_warren_cable_20251006')}}
)

select distinct * from consolidated
