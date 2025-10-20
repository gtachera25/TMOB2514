{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}
--runtime: 0.40s, 
with filtered_output as (
    select 
        ogc_fid as id,
        id as cable_or_fiber_id,
        market,
        case
            when market ilike '%Colorado%' then 'Colorado'
            when market ilike '%Minnesota%' then 'Minnesota'
            when market = 'Greenwood Village' then 'Colorado'
            when market = 'Cherry Creek' then 'Colorado'
            else 'Other'
        end as region,
        'intrepid' as owner,
        'Constructed' as status,
        case
            when folders ='strand_constructed_full' then 'fiber'
            when folders ='conduit_constructed_full' then 'conduit'
            else 'other'
        end as category,
        null as cable_or_fiber_length,
        geom
    from {{ref('base_intrepid_fiber_vac')}}
)
select distinct * from filtered_output