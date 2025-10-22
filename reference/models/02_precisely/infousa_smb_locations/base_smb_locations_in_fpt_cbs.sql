{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (business_source_id)",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime: 0.60s, rows =32792
with businesses_in_footprint as (
    select 
        distinct b.business_source_id,
        b.geom,
        f.census_block_code_2020,
        f.market_name,
        f.state,
        f.provider,
        f.status
    from {{ ref('base_smb_in_infousa') }} b
    inner join {{ ref('map_cbs_in_footprint') }} f
        on st_intersects(b.geom, f.geom)
),

market_designation_tagging as (
    select 
        distinct on (b.business_source_id)
        b.business_source_id,
        b.census_block_code_2020,
        b.market_name,
        b.state,
        b.provider,
        b.status,
        b.geom
    from businesses_in_footprint b
    order by b.business_source_id
)

select * from market_designation_tagging
