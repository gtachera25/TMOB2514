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
                state,
                 market,
                 predicted_penetration_60m_w_uplift
    from project_tmob2514.report_cbs_in_footprint_demos mcl 
    $REDSHIFT$) as t1 (
        state varchar(2),
        market varchar(15),
        predicted_penetration_60m_w_uplift float

    )
)

select 
a.*,
b.geom,
c.region
from dblink_cte a
left join {{ref('base_full_market_cbs')}} b
 on a.market = b.census_block_code_2020
left join {{ref('report_cbs_in_footprint')}} c
 on a.market = c.census_block_code_2020