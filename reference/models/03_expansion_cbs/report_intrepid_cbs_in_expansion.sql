{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)"
        ]
    )
}}

-- Calculate percentage of each CB that intersects with expansion polygons
with cb_to_polygon as (
    select 
        b.census_block_code_2020,
        a.ogc_fid,
        a.name as market_name,
        sum(ST_Area(ST_Intersection(a.geom, b.geom))::float / ST_Area(b.geom)::float) as pct_in_polygon
    from {{source('tmob2514','source_intrepid_polygon_expansion_in_place_10212025')}} a
    join {{ref('report_predicted_cbs_in_expansion_geoms')}} b
        on ST_Intersects(a.geom, b.geom)
    group by 1,2,3
),
cb_pct_in_footprint as (
    select 
        census_block_code_2020,
        LEAST(sum(pct_in_polygon), 1.0) as pct_in_footprint  -- Cap total at 100%
    from cb_to_polygon
    group by 1
),
cb_to_primary_polygon as (
    select distinct on (census_block_code_2020)
        census_block_code_2020,
        ogc_fid,
        market_name,
        pct_in_polygon
    from cb_to_polygon
    order by census_block_code_2020, pct_in_polygon desc
)
select
    a.census_block_code_2020,
    a.ogc_fid,
    a.market_name,
    a.pct_in_polygon,
    COALESCE(b.pct_in_footprint, 0) as pct_in_footprint
from cb_to_primary_polygon a
left join cb_pct_in_footprint b
    on a.census_block_code_2020 = b.census_block_code_2020
left join {{ref('report_predicted_cbs_in_expansion_geoms')}} c
    on a.census_block_code_2020 = c.census_block_code_2020

