{{ 
    config(
        materialized='table'
    ) 
}}

select a.*, b.geom
from {{ source('tmob2514', 'report_predicted_expansion_tracts_demos') }} a
join {{ source('tiger', 'tract') }} b
on a.census_tract_code = b.tract_id