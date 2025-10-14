{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom_buffered)"
        ]
    )
}}

SELECT
    ST_Transform(
        ST_Buffer(
            ST_Transform(geom, 3857), -- Transform to Web Mercator (meters)
            61                        -- 200 feet = 61 meters
        ), 
        4326                         -- Transform back to WGS84
    ) AS geom_buffered
FROM {{ ref('intrepid_union_geom') }}