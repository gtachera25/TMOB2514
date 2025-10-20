{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime = 0.92 s; 88953 rows returned
with one as(
select 
ogc_fid,
b.name as market,
a.folders,
a.id,
a.geom
from {{source('tmob2514','source_intrepid_network_vac_10162025')}} a
left join {{ ref('base_intrepid_polygons_consolidated') }} b
on st_intersects(a.geom, b.geom)
)
select *
from one
where market is not null