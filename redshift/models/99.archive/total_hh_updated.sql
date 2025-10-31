{{
    config(
        materialized='table'
    )
}}

with loc_id_level as (
    select 
        mc.census_cbsa_code,
        mc.cbsa_name,
        mc.cbsa_metro_or_micro_market,
        mc.census_tract_code,
        l.census_block_code_2020,
        l.location_id
    from {{ source('project_tmob2402', 'base_bdc_resi_comm_combined_cb_hocofinal_level_locations_rpt') }} l 
    left join {{ source('report_market', 'market_crosswalk_latest') }} mc 
        on l.census_block_code_2020 = mc.census_block_code_2020 
    where l.hocofinal is not null
),

num_locs_in_cb as (
    select 
        census_block_code_2020,
        count(distinct location_id) as num_locs_in_cb
    from loc_id_level
    group by 1
),

loc_level_w_num as (
    select 
        l.*,
        c.num_locs_in_cb
    from loc_id_level l 
    left join num_locs_in_cb c 
        on l.census_block_code_2020 = c.census_block_code_2020 
),

acs as (
    select 
        l.*,
        a.total_households,
        a.total_housing_units
    from loc_level_w_num l 
    join {{ source('report_acs', 'fact_acs_2023_census_block_demographics') }} a 
        on l.census_block_code_2020 = a.census_block_code_2020
),

hhs as (
    select 
        *,
        total_housing_units::real / num_locs_in_cb as total_housing_units_per_loc,
        total_households::real / num_locs_in_cb as total_households_per_loc 
    from acs 
)

select 
    census_cbsa_code,
    cbsa_name,
    cbsa_metro_or_micro_market,
    census_tract_code,
    sum(total_housing_units_per_loc) as total_housing_units,
    sum(total_households_per_loc) as total_households
from hhs
group by 1,2,3,4