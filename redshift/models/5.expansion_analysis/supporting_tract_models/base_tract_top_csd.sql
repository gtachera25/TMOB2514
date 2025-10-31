{{ 
    config(
        materialized='table',
        dist='census_tract_code',
        sort=['census_tract_code']
    )
}}

with tract_place_overlap as (
    select
        census_tract_code,
        census_county_subdivision_name,
        SUM(census_block_area_land_sq_miles) AS total_overlap_area
    from report_market.market_crosswalk_latest
    group by 1,2
),
row_num as (
    select
        census_tract_code,
        census_county_subdivision_name,
        total_overlap_area,
        row_number() over (partition by census_tract_code order by total_overlap_area desc) as rn
    from tract_place_overlap
)
select
    census_tract_code,
    census_county_subdivision_name,
    total_overlap_area
from row_num
where rn = 1