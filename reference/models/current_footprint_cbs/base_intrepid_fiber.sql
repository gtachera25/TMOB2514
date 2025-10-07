{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime: 0.40s, 148787 rows
with filtered_output as (
    select 
        ogc_fid as id,
        id as cable_or_fiber_id,
        v_project as market,
        case
            when v_project ilike '%co%' then 'Colorado'
            when v_project ilike '%mn%' then 'Minnesota'
            else 'Other'
        end as region,
        'intrepid' as owner,
        placement_ as status,
        case
            when folders ='strand_constructed' then 'fiber'
            when folders ='conduit_constructed' then 'conduit'
            else 'other'
        end as category,
        null as cable_or_fiber_length,
        geom
    from {{source('tmob2514', 'source_intrepid_cable_construction_20251007')}}
)
select distinct * from filtered_output