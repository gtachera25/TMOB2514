{{ 
    config(
        materialized='table'
    ) 
}}

with cb_to_tract_mapping as (
    select 
        a.census_block_code_2020,
        a.market_name,
        mkt.census_tract_code
    from {{ ref('report_predicted_cbs_in_expansion_ami_tagged') }} a
    left join {{ source('report_market','market_crosswalk_latest') }} mkt
        on a.census_block_code_2020 = mkt.census_block_code_2020
),
all_tracts as (
    select distinct census_tract_code
    from cb_to_tract_mapping
    where census_tract_code is not null
),
tract_market_counts as (
    select 
        census_tract_code,
        market_name,
        count(*) as blocks_with_market_name,
        count(*) over (partition by census_tract_code) as total_blocks_in_tract
    from cb_to_tract_mapping
    where market_name is not null
    group by census_tract_code, market_name
),
tract_primary_market as (
    select 
        census_tract_code,
        market_name,
        blocks_with_market_name,
        total_blocks_in_tract,
        blocks_with_market_name * 1.0 / total_blocks_in_tract as coverage_ratio,
        row_number() over (
            partition by census_tract_code 
            order by blocks_with_market_name desc, market_name
        ) as market_rank
    from tract_market_counts
    -- Remove the > 0.5 filter - just rank by coverage
)
select 
    t.census_tract_code,
    tm.market_name
from all_tracts t
left join tract_primary_market tm 
    on t.census_tract_code = tm.census_tract_code 
    and tm.market_rank = 1
