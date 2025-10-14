{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "create index on {{ this }} using gist(geom)",
            "alter table {{ this }}
                alter column geom type geometry(MULTILINESTRING, 4326) 
                USING ST_MakeValid(ST_Force2D(geom)::geometry(MULTILINESTRING, 4326))",
        ]
    )
}}

SELECT
    ST_Union(ST_MakeValid(geom)) AS geom
FROM {{ ref('base_intrepid_fiber') }}