{{ 
    config(
        materialized='table'
    ) 
}}
with dma_passings as(
select dma_name, sum(total_passings_in_footprint) from project_tmob2514.report_predicted_expansion_tract_demos_for_tagging
group by 1)
select dma_name,
case when sum > 1873594.4587127427 then 1 else 0 end as dma_greater_denver_flag
from dma_passings
group by 1,2