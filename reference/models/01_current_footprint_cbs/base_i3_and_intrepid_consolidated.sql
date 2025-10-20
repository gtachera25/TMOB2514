{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime = 0.92s, 137831 rows
with consolidated_markets as(
    select * from {{ ref('base_i3_illinois_consolidated') }}
    union all
    select * from {{ ref('base_i3_missuouri_consolidated') }}
    union all
    select * from {{ ref('base_i3_ri_consolidated') }}
    union all
    select * from {{ ref('base_intrepid_fiber') }}
)
select distinct * from consolidated_markets
