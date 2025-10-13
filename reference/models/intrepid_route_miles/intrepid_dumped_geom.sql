{{
    config(
        materialized='table',
        post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geom ON {{ this }} USING GIST (geom_part)"
    )
}}

SELECT
    ROW_NUMBER() OVER () as part_id,
    (ST_Dump(geom_buffered)).geom::geography AS geom_part
FROM {{ ref('intrepid_buffered_geom') }}