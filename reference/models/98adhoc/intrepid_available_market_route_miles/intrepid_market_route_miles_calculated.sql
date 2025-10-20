{{
    config(
        materialized='table'
    )
}}

SELECT
    SUM(ST_Perimeter(geom_part)) / 2 / 1609.34 AS route_miles
FROM {{ ref('intrepid_market_dumped_geom') }}