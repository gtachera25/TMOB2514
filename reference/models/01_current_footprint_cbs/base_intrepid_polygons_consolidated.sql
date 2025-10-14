{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}

with thornton as ( 
    select 'Thornton, CO' as name,
    geom 
    from {{ source('tmob2514','source_intrepid_thornton_co_10132025') }}
),
ifn_markets as(
    select folders as name,
    geom
    from {{ source('tmob2514','source_intrepid_ifn_markets_10132025') }}
),
greenwood as (
    select 
    name,
    geom
    from {{ source('tmob2514','source_intrepid_greenwood_co_10132025') }}
),
cherry_creek as (
    select 
    name,
    geom
    from {{ source('tmob2514','source_intrepid_cherry_creek_co_10132025') }}
),
final_union_select as(
select * from thornton
union all
select * from ifn_markets
union all
select * from greenwood
union all
select * from cherry_creek
)

select 
name,
'polygon' as source_type,
ST_Multi(ST_Transform(ST_MakeValid(geom), 4326))::geometry(MULTIPOLYGON, 4326) as geom from final_union_select