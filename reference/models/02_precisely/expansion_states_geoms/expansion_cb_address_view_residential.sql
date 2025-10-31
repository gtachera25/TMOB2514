{{ 
    config(
        materialized='table'
    ) 
}}
--runtime =1.18s, 2362869 rows returned
select *
from {{ source('tmob2514', 'az_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'ri_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'mo_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'mi_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'ca_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'ia_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'pa_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'ma_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'mn_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'co_cb_address_view_residential') }}
union all
select *
from {{ source('tmob2514', 'il_cb_address_view_residential') }}