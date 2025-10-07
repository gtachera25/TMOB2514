{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime: 0.14s, 10355 rows
with consolidated as (
select 
    ogc_fid as id,
    cableid as cable_or_fiber_id,
    market,
   'central illinois' as region,
    owner,   
    status,
    'cable' as category,
    "st_length(" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_central_illinois_cable_construction_20251006')}}
union all
select 
    ogc_fid as id,
    cableid as cable_or_fiber_id,
    market,
   'central illinois' as region,
    owner,  
    status,
    'cable' as category,
    "st_length(" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_central_illinois_cable_installed_20251006')}}
union all
select 
    ogc_fid as id,
    cableid as cable_or_fiber_id,
    market,
   'northern illinois' as region,
    owner,
    status,
    'cable' as category,
    "st_length(" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_northern_illinois_cable_construction_20251006')}}
union all
select 
    ogc_fid as id,
    cableid as cable_or_fiber_id,
    market,
   'northern illinois' as region,
    owner, 
    status,
    'cable' as category,
    "st_length(" as cable_or_fiber_length,
    geom
from {{source('tmob2514', 'source_i3_northern_illinois_cable_installed_20251006')}}
)

select distinct * from consolidated