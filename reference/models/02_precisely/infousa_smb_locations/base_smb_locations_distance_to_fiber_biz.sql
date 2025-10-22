{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)",
        ]
    )
}}

-- Time: ~9 mins
-- Rows: 7,660,993
-- Calculate the distance from each Precisely location to the nearest fiber line (within 1000ft buffer)

with relevant_network as (
    select
        *
    from {{ref('i3_intrepid_networks_buffered_biz')}}
)
select
    a.business_source_id,
    a.census_block_code_2020,
    a.market_name,
    a.state,
    a.provider,
    a.status,
    b.infra_type,
    a.geom,
    st_distance(a.geom::geography, b.geom::geography) / 0.3048 as distance_to_fiber_ft -- Meters to Feet
from {{ref('base_smb_locations_in_fpt_cbs')}} a
left join relevant_network b
    on st_intersects(a.geom, b.geom_buffer_1000ft)