{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
select 
a.census_block_code_2020,
 b.geom 
 from {{ source('tmob2514', 'cbs_in_footprint_missing_pen') }} a
 left join {{ref('base_full_market_cbs')}} b
 on a.census_block_code_2020 = b.census_block_code_2020