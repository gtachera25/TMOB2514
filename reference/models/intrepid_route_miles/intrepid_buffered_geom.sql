{{
    config(
        materialized='table',
        post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geom ON {{ this }} USING GIST (geom_buffered)"
    )
}}

SELECT
    ST_Buffer(geom_dissolved::geography, 200, 'endcap=square')::geometry AS geom_buffered
FROM {{ ref('intrepid_union_geom') }}