{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime: 0.20s, 17358 rows
with consolidated as (
select 
    ogc_fid as id,
    cableid as cable_or_fiber_id,
    market,
   'northern missouri' as region,
    owner,
    status,
    'cable' as category,
    "st_length(" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_northern_missouri_cable_construction_20251006')}}
union all
select 
    ogc_fid as id,
    cableid as cable_or_fiber_id,
    market,
   'northern missouri' as region,
    owner,   
    status,
    'cable' as category,
    "st_length(" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_northern_missouri_cable_installed_20251006')}}
union all
select
    ogc_fid as id,
    id as cable_or_fiber_id,
    v_project as market,
   'southern missouri' as region,
    'circle fiber' as owner, 
    v_status as status,
    case 
        when v_layer ilike '%Duct%' then 'conduit'
        when v_layer ilike '%Trunk%' then 'fiber'
        when v_layer ilike '%Strand%' then 'fiber'
        when v_layer ilike '%Distribution%' then 'fiber'
        else 'other'
    end as category,
    "total leng" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_southern_missouri_cable_20251006')}}
)
select distinct * from consolidated