{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)"
        ]
    )
}}
--runtime: 30.78s, 17099 rows
  select 
distinct on (a.census_block_code_2020)
    a.census_block_code_2020,
    b.market,
    b.region,
    a.state,
    b.owner,
    b.status,
    sum(st_length(st_intersection(a.geom, b.geom))) as intersection_length
from {{ ref('base_full_market_cbs') }} a
inner join {{ ref('base_i3_and_intrepid_consolidated') }} b
on st_intersects(a.geom, b.geom)
group by 1,2,3,4,5,6
order by a.census_block_code_2020, intersection_length desc