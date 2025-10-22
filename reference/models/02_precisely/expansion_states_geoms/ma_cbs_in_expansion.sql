{{ 
    config(
        materialized='table'
    ) 
}}

select *
from {{ ref('report_predicted_cbs_in_expansion_geoms') }} 
where state = 'MA'