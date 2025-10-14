{{ 
    config(
        materialized='table'
    ) 
}}

select 
cb_id,
(predicted_penetration_12m + pen_uplift_12m) as predicted_penetration_12m_uplift,
0 as predicted_penetration_36m_uplift,
(predicted_penetration_60m + (pen_uplift_12m/0.75)) as predicted_penetration_60m_uplift
from {{source('project_tmob2514','i3_penetration_cbs_excluding_tmo_as_comp_normal_10132025')}}