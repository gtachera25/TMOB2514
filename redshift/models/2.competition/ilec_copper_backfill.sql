{{
    config(
        materialized='table',
        dist='census_block_code_2020',
        sort=['census_block_code_2020']
    )
}}

with ilec_hocofinal_mapping as (
    select
        comp.*,
        map.hocofinal as ilec_hocofinal
    from {{ref('report_cb_provider_comp_coverage')}} comp
    left join {{ref('ilec_hocofinal_mapping')}} map
        on comp.ilec = map.ilec
),
missing_copper as (
    select
        census_block_code_2020,
        state,
        ilec,
        ilec_hocofinal,
        1 - sum(case when technology in ('Copper','Fiber','FWA') and hocofinal = ilec_hocofinal then percent_coverage else 0 end) as missing_copper_pct
    from ilec_hocofinal_mapping
    group by 1,2,3,4
)
select
    comp.*,
    0 as adjusted_flag
from {{ref('report_cb_provider_comp_coverage')}} comp
union all
select
    miss.census_block_code_2020,
    miss.state,
    1 as pct_in_footprint,
    cb.market_vac as market_mapping,
    cb.provider as provider_mapping,
    miss.ilec_hocofinal as hocofinal,
    'Copper' as technology,
    miss.missing_copper_pct as percent_coverage,
 	(acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) as total_passings,
    miss.ilec,
    1 as adjusted_flag
from missing_copper miss
left join {{ref('report_market_cbs')}} cb
    on miss.census_block_code_2020 = cb.census_block_code_2020
left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs
 	on miss.census_block_code_2020 = acs.census_block_code_2020
where miss.missing_copper_pct > 0
