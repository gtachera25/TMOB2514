{{ 
    config(
        materialized='table'
    ) 
}}

select 
a.*, b.market_name 
from {{ref('report_predicted_expansion_tract_demos')}} a
left join {{ref('expansion_cbs_to_tracts')}} b
    on a.census_tract_code = b.census_tract_code