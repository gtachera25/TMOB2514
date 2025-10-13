{{
    config(
        materialized='table',
        post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geom ON {{ this }} USING GIST (geom_dissolved)"
    )
}}

SELECT
    ST_Union(geom) AS geom_dissolved
FROM {{ ref('base_intrepid_fiber') }}