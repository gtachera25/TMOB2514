{{
    config(
        materialized='table',
        dist='census_block_code_2020',
        sort=['census_block_code_2020']
    )
}}

-- Time: 34 seconds
-- CB x Provider x Tech Level Coverage

with coverage_by_tech as (
	select
		a.census_block_code_2020,
		b.hocofinal,
		'Copper' as technology,
		pct_copper_served_locations as percent_coverage
	from {{ref('report_market_cbs')}} a
	left join {{source('report_bdc','nbm_broadband_current_residential_census_block_hocofinal_level')}} b
		on a.census_block_code_2020 = b.census_block_code_2020
	where pct_copper_served_locations > 0 -- Leave off the speed threshold for copper
		and hocofinal not in ('WH i3B Bidco LLC/WH i3B Topco, LLC', 'BIF IV Intrepid HoldCo, LLC')
	union all
	select
		a.census_block_code_2020,
		b.hocofinal,
		'Fiber' as technology,
		pct_fiber_served_locations as percent_coverage
	from {{ref('report_market_cbs')}} a
	left join {{source('report_bdc','nbm_broadband_current_residential_census_block_hocofinal_level')}} b
		on a.census_block_code_2020 = b.census_block_code_2020
	where pct_fiber_served_locations > 0 and fiber_max_down_speed >= 100 and fiber_max_up_speed >= 20
		and hocofinal not in ('WH i3B Bidco LLC/WH i3B Topco, LLC', 'BIF IV Intrepid HoldCo, LLC')
	union all
	select
		a.census_block_code_2020,
		b.hocofinal,
		'Cable' as technology,
		pct_cable_served_locations as percent_coverage
	from {{ref('report_market_cbs')}} a
	left join {{source('report_bdc','nbm_broadband_current_residential_census_block_hocofinal_level')}} b
		on a.census_block_code_2020 = b.census_block_code_2020
	where pct_cable_served_locations > 0 and cable_max_down_speed >= 100 and cable_max_up_speed >= 20
		and hocofinal not in ('WH i3B Bidco LLC/WH i3B Topco, LLC', 'BIF IV Intrepid HoldCo, LLC')
	union all
	select
		a.census_block_code_2020,
		b.hocofinal,
		'FWA' as technology,
		pct_fw_served_locations as percent_coverage
	from {{ref('report_market_cbs')}} a
	left join {{source('report_bdc','nbm_broadband_current_residential_census_block_hocofinal_level')}} b
		on a.census_block_code_2020 = b.census_block_code_2020
	where pct_fw_served_locations > 0 and fw_max_down_speed >= 900 and fw_max_up_speed >= 20
		and hocofinal not in ('WH i3B Bidco LLC/WH i3B Topco, LLC', 'BIF IV Intrepid HoldCo, LLC')
)
select
	cbs.census_block_code_2020,
	cbs.state,
	1 as pct_in_footprint,
	cbs.market_vac as market_mapping,
	cbs.provider as provider_mapping,
	a.hocofinal,
	a.technology,
	a.percent_coverage,
	(acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) as total_passings,
	mkt.ilec
from {{ref('report_market_cbs')}} cbs
left join coverage_by_tech a
	on cbs.census_block_code_2020 = a.census_block_code_2020
left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs
	on cbs.census_block_code_2020 = acs.census_block_code_2020
left join {{source('report_market','market_crosswalk_latest')}} mkt
	on cbs.census_block_code_2020 = mkt.census_block_code_2020