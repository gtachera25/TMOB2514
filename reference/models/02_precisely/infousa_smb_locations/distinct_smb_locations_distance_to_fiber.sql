{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (business_source_id)",
            "create index on {{ this }} using gist(geom)",
        ]
    )
}}
--runtime :0.92 s, rows = 32,792
select
    distinct on (business_source_id)
    *,
    case
		when distance_to_fiber_ft < 100 then '01. <100ft'
		when distance_to_fiber_ft < 200 then '02. 100ft-200ft'
        when distance_to_fiber_ft < 300 then '03. 200ft-300ft'
        when distance_to_fiber_ft < 400 then '04. 300ft-400ft'
        when distance_to_fiber_ft < 500 then '05. 400ft-500ft'
        when distance_to_fiber_ft < 600 then '06. 500ft-600ft'
        when distance_to_fiber_ft < 700 then '07. 600ft-700ft'
        when distance_to_fiber_ft < 800 then '08. 700ft-800ft'
        when distance_to_fiber_ft < 900 then '09. 800ft-900ft'
        when distance_to_fiber_ft < 1000 then '10. 900ft-1000ft'
        else '11. >1000ft'
	end as distance_bucket
from {{ref('base_smb_locations_distance_to_fiber_biz')}} a
order by business_source_id, distance_to_fiber_ft asc