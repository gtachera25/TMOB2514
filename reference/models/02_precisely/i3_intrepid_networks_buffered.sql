{{
    config(
        materialized='table',
        post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geom ON {{ this }} USING GIST (geom)"
    )
}}


-- 1) Union fiber into a single geometry (avoids overlapping buffers)
WITH fiber AS (
  SELECT ST_UnaryUnion(geom) AS geom
from {{ref('i3_intrepid_networks')}}
),

-- 2) Build cumulative buffers at 100,200,300,...,1000 ft (feet → meters = *0.3048)
bands AS (
  SELECT dist_ft,
         ST_Buffer(f.geom::geography, dist_ft * 0.3048)::geometry AS geom_buffer
  FROM fiber f
  CROSS JOIN generate_series(100, 1000, 100) AS dist_ft
),

-- 3) Convert cumulative buffers to rings by subtracting the previous band
rings AS (
  SELECT
    dist_ft,
    CASE
      WHEN dist_ft = 100
        THEN ST_Multi(geom_buffer)
      ELSE
        ST_Multi(ST_Difference(geom_buffer,
               LAG(geom_buffer) OVER (ORDER BY dist_ft)))
    END AS geom
  FROM bands
)

-- 4) Materialize with band labels matching the distance categories
SELECT
  dist_ft,
  CASE
    WHEN dist_ft = 100 THEN '01. <100ft'
    WHEN dist_ft = 200 THEN '02. 100ft-200ft'
    WHEN dist_ft = 300 THEN '03. 200ft-300ft'
    WHEN dist_ft = 400 THEN '04. 300ft-400ft'
    WHEN dist_ft = 500 THEN '05. 400ft-500ft'
    WHEN dist_ft = 600 THEN '06. 500ft-600ft'
    WHEN dist_ft = 700 THEN '07. 600ft-700ft'
    WHEN dist_ft = 800 THEN '08. 700ft-800ft'
    WHEN dist_ft = 900 THEN '09. 800ft-900ft'
    WHEN dist_ft = 1000 THEN '10. 900ft-1000ft'
  END AS band_label,
  geom
FROM rings


