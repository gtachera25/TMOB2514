{{
    config(
        materialized='table'
    )
}}

with distinct_cbs as (
	select
		distinct market_mapping,
		provider_mapping,
		census_block_code_2020,
		pct_in_footprint,
		total_passings
	from {{ref('ilec_copper_backfill')}}
),
market_level_totals as (
	select
		market_mapping,
		provider_mapping,
		sum(total_passings * pct_in_footprint) as total_passings_in_footprint
	from distinct_cbs
	group by 1,2
),
provider_rollup as (
	select
		market_mapping,
		provider_mapping,
		hocofinal,
		technology,
		sum(total_passings * pct_in_footprint * percent_coverage) as covered_units
	from {{ref('ilec_copper_backfill')}}
	group by 1,2,3,4
)
select
	a.market_mapping,
	a.hocofinal,
	a.technology,
	a.provider_mapping,
	b.total_passings_in_footprint as provider_covered_units,
	a.covered_units as competitor_covered_units,
	covered_units::float / total_passings_in_footprint::float as pct_competitor_coverage
from provider_rollup a
left join market_level_totals b
	on a.market_mapping = b.market_mapping and a.provider_mapping = b.provider_mapping
order by 1,2,3,4