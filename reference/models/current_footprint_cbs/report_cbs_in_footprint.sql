{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)"
        ]
    )
}}
--runtime =10.69, rows returned =2,537,668
with intersections as (
    select 
        a.census_block_code_2020,
        b.market,
        b.region,
        a.state,
        b.owner,
        b.status,
        st_length(st_intersection(a.geom, b.geom)) as intersection_length,
        row_number() over (
            partition by a.census_block_code_2020 
            order by st_length(st_intersection(a.geom, b.geom)) desc
        ) as rn
    from {{ ref('base_full_market_cbs') }} a
    left join {{ ref('base_i3_and_intrepid_consolidated') }} b
    on st_intersects(a.geom, b.geom)
)

select 
    census_block_code_2020,
    market,
    region,
    state,
    owner,
    status
from intersections
where rn = 1 or intersection_length is null