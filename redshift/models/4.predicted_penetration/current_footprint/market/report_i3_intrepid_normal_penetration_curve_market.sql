{{ 
    config(
        materialized='table',
        dist='market_mapping',
        sort=['market_mapping']
    )
}}


with relevant_cbs as (
	select
		cb.census_block_code_2020,
    case 
        when cb.owner = 'intrepid' then cb.markets
        when cb.region ='Rhode Island' then cb.markets
        else cb.region 
    end as market_mapping,
    case when cb.owner in ('UC2B', 'i3', 'Consolidated', 'circle fiber','BBN', 'Stratus Networks') then 'WH i3B Bidco LLC/WH i3B Topco, LLC'
    else 'BIF IV Intrepid HoldCo, LLC'
    end as provider,
		cb.state,
		1 as pct_in_footprint,
		(acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) as total_passings
	from {{source('project_tmob2514','report_cbs_in_footprint')}} cb
	left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs
		on cb.census_block_code_2020 = acs.census_block_code_2020
)
select
	market_mapping,
    provider,
	state,
	sum(months_since_ofs_0 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_0,
    sum(months_since_ofs_1 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_1,
    sum(months_since_ofs_2 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_2,
    sum(months_since_ofs_3 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_3,
    sum(months_since_ofs_4 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_4,
    sum(months_since_ofs_5 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_5,
    sum(months_since_ofs_6 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_6,
	sum(months_since_ofs_7 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_7,
	sum(months_since_ofs_8 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_8,
	sum(months_since_ofs_9 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_9,
	sum(months_since_ofs_10 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_10,
	sum(months_since_ofs_11 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_11,
	sum(months_since_ofs_12 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_12,
	sum(months_since_ofs_13 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_13,
	sum(months_since_ofs_14 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_14,
	sum(months_since_ofs_15 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_15,
	sum(months_since_ofs_16 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_16,
	sum(months_since_ofs_17 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_17,
	sum(months_since_ofs_18 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_18,
	sum(months_since_ofs_19 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_19,
	sum(months_since_ofs_20 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_20,
	sum(months_since_ofs_21 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_21,
	sum(months_since_ofs_22 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_22,
	sum(months_since_ofs_23 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_23,
	sum(months_since_ofs_24 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_24,
	sum(months_since_ofs_25 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_25,
	sum(months_since_ofs_26 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_26,
	sum(months_since_ofs_27 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_27,
	sum(months_since_ofs_28 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_28,
	sum(months_since_ofs_29 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_29,
	sum(months_since_ofs_30 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_30,
	sum(months_since_ofs_31 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_31,
	sum(months_since_ofs_32 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_32,
	sum(months_since_ofs_33 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_33,
	sum(months_since_ofs_34 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_34,
	sum(months_since_ofs_35 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_35,
	sum(months_since_ofs_36 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_36,
	sum(months_since_ofs_37 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_37,
	sum(months_since_ofs_38 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_38,
	sum(months_since_ofs_39 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_39,
	sum(months_since_ofs_40 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_40,
	sum(months_since_ofs_41 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_41,
	sum(months_since_ofs_42 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_42,
	sum(months_since_ofs_43 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_43,
	sum(months_since_ofs_44 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_44,
	sum(months_since_ofs_45 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_45,
	sum(months_since_ofs_46 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_46,
	sum(months_since_ofs_47 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_47,
	sum(months_since_ofs_48 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_48,
	sum(months_since_ofs_49 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_49,
	sum(months_since_ofs_50 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_50,
	sum(months_since_ofs_51 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_51,
	sum(months_since_ofs_52 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_52,
	sum(months_since_ofs_53 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_53,
	sum(months_since_ofs_54 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_54,
	sum(months_since_ofs_55 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_55,
	sum(months_since_ofs_56 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_56,
	sum(months_since_ofs_57 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_57,
	sum(months_since_ofs_58 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_58,
	sum(months_since_ofs_59 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_59,
	sum(months_since_ofs_60 * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as months_since_ofs_60
from {{source('project_tmob2514','i3_intrepid_census_block_pen_model_0_60_month_10142025')}} pen
join relevant_cbs cb
	on pen.cb_id = cb.census_block_code_2020
where curve_method = 'normal'
group by 1,2,3