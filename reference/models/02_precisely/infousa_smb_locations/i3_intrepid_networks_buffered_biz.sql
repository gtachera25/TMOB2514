{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)",
            "create index on {{ this }} using gist(geom_buffer_1000ft)"
        ]
    )
}}

-- Time:
-- Rows:
-- Choose the relevant parts of the network from both GL and GNS (Fiber)

select
    *,
    st_buffer(geom::geography, 304.8)::geometry as geom_buffer_1000ft
from {{ref('i3_intrepid_networks')}}