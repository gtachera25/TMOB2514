{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}

with dblink_cte as (
    select * from dblink('deltacity', $REDSHIFT$
                select
                 market,
                 total_passings_in_footprint
    from project_tmob2514.report_cbs_in_polygons_demos mcl 
    $REDSHIFT$) as t1 (
        market varchar(256),
        total_passings_in_footprint float

    )
)

select 
a.*,
b.geom
from dblink_cte a
left join {{ref('base_full_market_cbs')}} b
 on a.market = b.census_block_code_2020