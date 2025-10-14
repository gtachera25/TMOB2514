{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)"
        ]
    )
}}

    -- Polygon infrastructure - use area
    SELECT 
    distinct on (b.tabblock_id)
        b.tabblock_id as census_block_code_2020,
        a.source_type,
        a.name as markets,
        case
        when a.name ilike '%Minnesota%' then 'Minnesota'
        when a.name ilike '%Colorado%' then 'Colorado'
        when a.name ilike '%Thornton, CO%' then 'Colorado'
        when a.name ilike '%Greenwood Village%' then 'Colorado'
        when a.name ilike '%Cherry Creek%' then 'Colorado'
        else 'Other' end as region,
        'intrepid' as owner,
        null as status,
        st_area(st_intersection(a.geom, b.geom)) as intersection_metric
    FROM {{ ref('base_intrepid_polygons_consolidated') }} a
    INNER JOIN {{ source('tiger','tabblock') }} b
    ON st_intersects(a.geom, b.geom)
    ORDER BY b.tabblock_id, intersection_metric DESC
