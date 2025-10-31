{{ 
    config(
        materialized='table',
        dist='market',
        sort=['market']
    )
}}

-- Time: 253 seconds

-- Define Desired Market Level
with market_definition as (
    select
        fpt.census_block_code_2020,
        mkt.census_tract_code || ' || ' || mkt.state || ' || ' || fpt.ami_tag || ' || ' || fpt.designation as market,
        csd.census_county_subdivision_name as csd_name,
        mkt.county_name as county,
        mkt.cbsa_name as metro_name,
        dm.dma_name,
        a.county_passings_less_5k_flag,
        b.dma_greater_denver_flag,
        1 as pct_in_footprint -- Switch to 1 for National Level
    from {{ ref('report_predicted_cbs_in_expansion_ami_tagged') }} fpt
    left join {{ source('report_market','market_crosswalk_latest') }} mkt
        on fpt.census_block_code_2020 = mkt.census_block_code_2020
    left join {{ ref('base_tract_top_dma') }} dm
        on mkt.census_tract_code = dm.census_tract_code
    left join {{ ref('base_tract_top_csd') }} csd
        on mkt.census_tract_code = csd.census_tract_code
    left join {{ref('county_passings_less_5k')}} a
        on mkt.county_name = a.county
    left join {{ref('dma_greater_denver_flag')}} b
        on dm.dma_name = b.dma_name
),
-- Define BDC Location to Passings Ratio
bdc_counts as (
	select
		def.market,
        def.census_block_code_2020,
		count(distinct bdc.location_id) as bdc_locs
	from market_definition def
	left join {{source('report_bdc','fact_bdc_location_reporting_current')}} bdc
		on def.census_block_code_2020 = bdc.census_block_code_2020
	group by 1,2
),
bdc_to_passings as (
	select 
	    distinct bdc.census_block_code_2020,
	    (acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) * 1.0 / nullif(bdc_locs,0) as bdc_to_passings -- Ensures we count coverage proportional to ACS HUs
	from bdc_counts bdc
	join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs
		on bdc.census_block_code_2020 = acs.census_block_code_2020
),
-- Create base table of CBs
base_cbs as (
    select
        def.*,
        bdc.bdc_to_passings,
        (acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) as total_passings
    from market_definition def
    left join bdc_to_passings bdc 
        on def.census_block_code_2020 = bdc.census_block_code_2020
    left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs
        on def.census_block_code_2020 = acs.census_block_code_2020
),
-- Market Summary
market_summary as (
    select
        market,
        csd_name,
        county,
        metro_name,
        dma_name,
        county_passings_less_5k_flag,
        dma_greater_denver_flag,
       ---------------------------------------
        count(distinct census_block_code_2020) as cb_count,
        sum(total_passings) as total_passings_in_market,
        sum(total_passings * pct_in_footprint) as total_passings_in_footprint
    from base_cbs
    group by 1,2,3,4,5,6,7
),
-- Morphology
morphology as (
	select 
        cb.market,
		sum(case when mkt.cbg_morphology = 'Rural' then cb.total_passings * pct_in_footprint else 0 end) * 1.0 / nullif(sum(cb.total_passings * pct_in_footprint),0) as pct_rural,
		sum(case when mkt.cbg_morphology = 'Suburban' then cb.total_passings * pct_in_footprint else 0 end) * 1.0 / nullif(sum(cb.total_passings * pct_in_footprint),0) as pct_suburban,
		sum(case when mkt.cbg_morphology = 'Urban' then cb.total_passings * pct_in_footprint else 0 end) * 1.0 / nullif(sum(cb.total_passings * pct_in_footprint),0) as pct_urban,
		sum(case when mkt.cbg_morphology = 'Dense Urban' then cb.total_passings * pct_in_footprint else 0 end) * 1.0 / nullif(sum(cb.total_passings * pct_in_footprint),0) as pct_dense_urban
	from base_cbs cb
    join {{source('report_market','market_crosswalk_latest')}} mkt
        on cb.census_block_code_2020 = mkt.census_block_code_2020
	group by 1
),
-- ACS 2023 Metrics
acs_metrics as (
    select
        cb.market,
        sum(acs.total_housing_units * pct_in_footprint) as total_housing_units,
        sum(acs.total_households * pct_in_footprint) as total_households,
        sum(acs.total_population * pct_in_footprint) as total_population,
        -- SFUs/MDUs
        (sum(acs.total_housing_units_units_in_structure_1_detached * pct_in_footprint)
            + sum(acs.total_housing_units_units_in_structure_1_attached * pct_in_footprint))
            as units_1,
        (sum(total_housing_units_units_in_structure_2 * pct_in_footprint)
            + sum(total_housing_units_units_in_structure_3_or_4 * pct_in_footprint))
            as units_2_4,
        (sum(total_housing_units_units_in_structure_5_to_9 * pct_in_footprint)+
            + sum(total_housing_units_units_in_structure_10_to_19 * pct_in_footprint))
            as units_5_19,
        (sum(total_housing_units_units_in_structure_20_to_49 * pct_in_footprint) +
         sum(total_housing_units_units_in_structure_50_or_more * pct_in_footprint)) 
         as units_20_plus,
        sum(total_housing_units_units_in_structure_mobile_home * pct_in_footprint)
            as units_mobile_home,
        sum(total_housing_units_units_in_structure_other_boat_rv_van * pct_in_footprint)
            as units_other_boat_rv_van,
        -- HH Metrics
        sum(acs.total_housing_units_occupied * pct_in_footprint) / nullif(sum(acs.total_housing_units * pct_in_footprint),0) as occupancy_rate,
        sum(acs.total_housing_units_vacant_for_seasonal_recreational_occasional_use * pct_in_footprint) / nullif(sum(acs.total_housing_units * pct_in_footprint),0) as seasonal_occupancy_rate,
        (sum(acs.households_15k_20k_income * pct_in_footprint) + sum(acs.households_10k_15k_income * pct_in_footprint) + sum(acs.households_less_than_10k_income * pct_in_footprint)) / nullif(sum(acs.total_households * pct_in_footprint),0) as pct_hhs_income_below_20k,
        sum(acs.total_housing_units_occupied_renter_occupied * pct_in_footprint) / nullif(sum(acs.total_housing_units_occupied * pct_in_footprint),0) as pct_rented,
        sum(acs.median_home_value * acs.total_households * pct_in_footprint) / nullif(sum(case when acs.median_home_value is not null then acs.total_households * pct_in_footprint end),0) as median_home_value,
        sum(acs.median_household_income * acs.total_households * pct_in_footprint) / nullif(sum(case when acs.median_household_income is not null then acs.total_households * pct_in_footprint end),0) as median_household_income,
        sum(acs.total_population * pct_in_footprint)/ nullif(sum(acs.total_housing_units_occupied * pct_in_footprint),0) as avg_hh_size,
        sum(acs.total_households_with_any_broadband * pct_in_footprint) / nullif(sum(acs.total_households * pct_in_footprint),0) as pct_households_with_any_broadband,
        -- Population Metrics
        (sum(acs.population_25_plus_years_doctorate_degree * pct_in_footprint) + sum(acs.population_25_plus_years_masters_degree_only * pct_in_footprint) + sum(acs.population_25_plus_years_professional_school_degree_only * pct_in_footprint) + sum(acs.population_25_plus_years_bachelors_degree_only * pct_in_footprint))/nullif(sum(acs.population_25_plus_years * pct_in_footprint), 0) as higher_ed_rate,
        sum(acs.total_population_hispanic_or_latino_origin_total_hispanic_or_latino * pct_in_footprint) / nullif(sum(acs.total_population_hispanic_or_latino_origin_total * pct_in_footprint), 0) as pct_hispanic_or_latino_origin,
        sum(acs.median_age * acs.total_population * pct_in_footprint) / nullif(sum(case when acs.median_age is not null then acs.total_population * pct_in_footprint end),0) as median_age,
        sum(acs.population_16_yrs_plus_in_civilian_labor_force_unemployed * pct_in_footprint) / nullif(sum(acs.population_16_yrs_plus_in_civilian_labor_force * pct_in_footprint), 0) as unemployment_rate,
        -- 0-14
        (sum(acs.total_population_under_5_yrs_male * pct_in_footprint)
            + sum(acs.total_population_5_to_9_yrs_male * pct_in_footprint)
            + sum(acs.total_population_10_to_14_yrs_male * pct_in_footprint)
            + sum(acs.total_population_under_5_yrs_female * pct_in_footprint)
            + sum(acs.total_population_5_to_9_yrs_female * pct_in_footprint)
            + sum(acs.total_population_10_to_14_yrs_female * pct_in_footprint))
            / nullif(sum(acs.total_population * pct_in_footprint),0) as pct_pop_age_0_14,
        -- 15-17
        (sum(acs.total_population_15_to_17_yrs_male * pct_in_footprint)
            + sum(acs.total_population_15_to_17_yrs_female * pct_in_footprint))
            / nullif(sum(acs.total_population * pct_in_footprint),0) as pct_pop_age_15_17,
        -- 18-29
        (sum(acs.total_population_18_to_19_yrs_male * pct_in_footprint)
            + sum(acs.total_population_20_yrs_male * pct_in_footprint)
            + sum(acs.total_population_21_yrs_male * pct_in_footprint)
            + sum(acs.total_population_22_to_24_yrs_male * pct_in_footprint)
            + sum(acs.total_population_25_to_29_yrs_male * pct_in_footprint)
            + sum(acs.total_population_18_to_19_yrs_female * pct_in_footprint)
            + sum(acs.total_population_20_yrs_female * pct_in_footprint)
            + sum(acs.total_population_21_yrs_female * pct_in_footprint)
            + sum(acs.total_population_22_to_24_yrs_female * pct_in_footprint)
            + sum(acs.total_population_25_to_29_yrs_female * pct_in_footprint))
            / nullif(sum(acs.total_population * pct_in_footprint),0) as pct_pop_age_18_29,
        -- 30-44
        (sum(acs.total_population_30_to_34_yrs_male * pct_in_footprint)
            + sum(acs.total_population_35_to_39_yrs_male * pct_in_footprint)
            + sum(acs.total_population_40_to_44_yrs_male * pct_in_footprint)
            + sum(acs.total_population_30_to_34_yrs_female * pct_in_footprint)
            + sum(acs.total_population_35_to_39_yrs_female * pct_in_footprint)
            + sum(acs.total_population_40_to_44_yrs_female * pct_in_footprint))
            / nullif(sum(acs.total_population * pct_in_footprint),0) as pct_pop_age_30_44,
        -- 45-64
        (sum(acs.total_population_45_to_49_yrs_male * pct_in_footprint)
            + sum(acs.total_population_50_to_54_yrs_male * pct_in_footprint)
            + sum(acs.total_population_55_to_59_yrs_male * pct_in_footprint)
            + sum(acs.total_population_60_to_61_yrs_male * pct_in_footprint)
            + sum(acs.total_population_62_to_64_yrs_male * pct_in_footprint)
            + sum(acs.total_population_45_to_49_yrs_female * pct_in_footprint)
            + sum(acs.total_population_50_to_54_yrs_female * pct_in_footprint)
            + sum(acs.total_population_55_to_59_yrs_female * pct_in_footprint)
            + sum(acs.total_population_60_to_61_yrs_female * pct_in_footprint)
            + sum(acs.total_population_62_to_64_yrs_female * pct_in_footprint))
            / nullif(sum(acs.total_population * pct_in_footprint),0) as pct_pop_age_45_64,
        -- 65+
        (sum(acs.total_population_65_to_66_yrs_male * pct_in_footprint)
            + sum(acs.total_population_67_to_69_yrs_male * pct_in_footprint)
            + sum(acs.total_population_70_to_74_yrs_male * pct_in_footprint)
            + sum(acs.total_population_75_to_79_yrs_male * pct_in_footprint)
            + sum(acs.total_population_80_to_84_yrs_male * pct_in_footprint)
            + sum(acs.total_population_85_plus_yrs_male * pct_in_footprint)
            + sum(acs.total_population_65_to_66_yrs_female * pct_in_footprint)
            + sum(acs.total_population_67_to_69_yrs_female * pct_in_footprint)
            + sum(acs.total_population_70_to_74_yrs_female * pct_in_footprint)
            + sum(acs.total_population_75_to_79_yrs_female * pct_in_footprint)
            + sum(acs.total_population_80_to_84_yrs_female * pct_in_footprint)
            + sum(acs.total_population_85_plus_yrs_female * pct_in_footprint))
            / nullif(sum(acs.total_population * pct_in_footprint),0) as pct_pop_age_65_plus
    from base_cbs cb
    left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs
        on cb.census_block_code_2020 = acs.census_block_code_2020
    group by 1
),
-- ACS 2016 Reweighted to 2020 CBs
acs_2016_onto_2020 as (
    select
        cb.market,
        sum(map.proportion_cb_2010 * acs16.total_households * pct_in_footprint) as total_households_2016,
        sum(map.proportion_cb_2010 * acs16.total_housing_units * pct_in_footprint) as total_housing_units_2016,
        sum(map.proportion_cb_2010 * acs16.total_population * pct_in_footprint) as total_population_2016
    from {{ source('report_acs','fact_acs_2016_census_block_demographics') }} acs16
    join {{ source('source_market','census_block_2010_to_census_block_2020_proportional') }} map
        on map.census_block_code_2010 = acs16.census_block_code
    join base_cbs cb
        on cb.census_block_code_2020 = map.census_block_code_2020
    group by 1
),
-- SMBs
businesses as (
    select
        census_block_code_2020,
        count(distinct business_source_id) as smb_count
    from {{source('report_business','fact_businesses_2025')}} fb
    where emps < 25
    group by 1
),
businesses_by_market as (
    select
        cb.market,
        sum(smb_count * pct_in_footprint) as smb_count
    from base_cbs cb
    left join businesses biz
        on cb.census_block_code_2020 = biz.census_block_code_2020
    group by 1
),
-- Road Mile Density
road_mile_density_cbg as (
    select 
        acs.census_block_group_code, 
        rm.secondary_road_miles + rm.local_road_miles as road_miles,
        (acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) / nullif(rm.secondary_road_miles + rm.local_road_miles,0) as cbg_road_mile_density
    FROM {{source('report_acs','fact_acs_2023_census_block_group_demographics')}} acs 
    JOIN {{source('source_market','road_miles_by_census_block_group_2024')}} rm
        on acs.census_block_group_code = rm.census_block_group_code
),
road_mile_density_cb as (
    select
        acs.census_block_code_2020,
        rm.cbg_road_mile_density,
        (acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) / nullif(rm.cbg_road_mile_density,0) as implied_road_miles
    from {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs 
    left join {{source('report_market','market_crosswalk_latest')}} mkt
        on acs.census_block_code_2020 = mkt.census_block_code_2020
    left join road_mile_density_cbg rm
        on mkt.census_block_group_code = rm.census_block_group_code
),
road_mile_density_market as (
    select
        cb.market,
        sum((acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) * pct_in_footprint) / nullif(sum(implied_road_miles * pct_in_footprint),0) as road_mile_density -- Check with team on numerator
    from base_cbs cb
    left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs 
        on cb.census_block_code_2020 = acs.census_block_code_2020
    left join road_mile_density_cb rm
        on cb.census_block_code_2020 = rm.census_block_code_2020
    group by 1
),
road_mile_density_market_morphology as (
    select
        cb.market,
        mkt.cbg_morphology,
        --sum(implied_road_miles * pct_in_footprint) as road_miles,
        sum((acs.total_housing_units - acs.total_housing_units_units_in_structure_mobile_home - acs.total_housing_units_units_in_structure_other_boat_rv_van) * pct_in_footprint) / nullif(sum(implied_road_miles * pct_in_footprint),0) as road_mile_density -- Check with team on numerator
    from base_cbs cb
    left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs 
        on cb.census_block_code_2020 = acs.census_block_code_2020
    left join road_mile_density_cb rm
        on cb.census_block_code_2020 = rm.census_block_code_2020
    left join {{source('report_market','market_crosswalk_latest')}} mkt
        on cb.census_block_code_2020 = mkt.census_block_code_2020
    group by 1,2
),
road_mile_density_final_morphology as(
    select
        market,
        sum(case when cbg_morphology = 'Rural' then road_mile_density else 0 end) as rural_road_mile_density,
        sum(case when cbg_morphology = 'Suburban' then road_mile_density  else 0 end) as suburban_road_mile_density,
        sum(case when cbg_morphology = 'Urban' then road_mile_density else 0 end) as urban_road_mile_density,
        sum(case when cbg_morphology = 'Dense Urban' then road_mile_density else 0 end) as dense_urban_road_mile_density
    from road_mile_density_market_morphology
    group by 1
),
-- Language Spoken at Home
language_speakers_tract_level as (
    select
        census_tract_code,
        pop_over_5_yrs_speak_only_english / nullif(pop_over_5_yrs, 0) as pct_english_only_speakers,
        pop_over_5_yrs_speak_language_other_than_english / nullif(pop_over_5_yrs, 0) as pct_other_language_speakers,
        pop_over_5_yrs_speak_language_other_than_english_spanish / nullif(pop_over_5_yrs, 0) as pct_other_language_speakers_spanish
    from {{source('project_tmob2513','source_acs_tract_language_spoken_at_home_2023')}}
),
language_speakers_cb_level as (
    select
        mkt.census_block_code_2020,
        pct_english_only_speakers,
        pct_other_language_speakers,
        pct_other_language_speakers_spanish
    from {{source('report_market','market_crosswalk_latest')}} mkt
    join language_speakers_tract_level lan
        on mkt.census_tract_code = lan.census_tract_code
),
language_speakers_market_level as (
    select
        cb.market,
        sum((total_population - (total_population_under_5_yrs_male + total_population_under_5_yrs_female)) * pct_english_only_speakers * pct_in_footprint) / nullif(sum((total_population - (total_population_under_5_yrs_male + total_population_under_5_yrs_female)) * pct_in_footprint),0) as pct_english_only_speakers,
        sum((total_population - (total_population_under_5_yrs_male + total_population_under_5_yrs_female)) * pct_other_language_speakers * pct_in_footprint) / nullif(sum((total_population - (total_population_under_5_yrs_male + total_population_under_5_yrs_female)) * pct_in_footprint),0) as pct_other_language_speakers,
        sum((total_population - (total_population_under_5_yrs_male + total_population_under_5_yrs_female)) * pct_other_language_speakers_spanish * pct_in_footprint) / nullif(sum((total_population - (total_population_under_5_yrs_male + total_population_under_5_yrs_female)) * pct_in_footprint),0) as pct_other_language_speakers_spanish
    from base_cbs cb
    left join {{source('report_acs','fact_acs_2023_census_block_demographics')}} acs 
        on cb.census_block_code_2020 = acs.census_block_code_2020
    left join language_speakers_cb_level lan
        on cb.census_block_code_2020 = lan.census_block_code_2020
    group by 1
),
-- BDC Provider Coverage
bdc_by_provider as (
    select 
        * 
    from base_cbs cb
    join report_bdc.fact_bdc_location_reporting_current bdc
        using(census_block_code_2020)
),
unique_bdc_locations as (
    select 
        distinct market,
        census_block_code_2020,
        pct_in_footprint,
        location_id,
        bdc_to_passings
    from bdc_by_provider
),
bdc_market_denominator as (
    select 
        market,
        sum(bdc_to_passings * pct_in_footprint) as market_denominator
    from unique_bdc_locations
    group by 1
),
bdc_market_provider as (
    select 
        market,
        hocofinal,
        case
            when resi_tech_code = '50' then 'Fiber'
            when resi_tech_code = '40' then 'Cable'
            else 'Other'
        end as technology,
        sum(bdc_to_passings * pct_in_footprint) as numerator
    from bdc_by_provider bdc
    where resi_tech_code in ('40', '50')
        and resi_down_speed >= 100
            and resi_up_speed >= 20
            and hocofinal not in ('WH i3B Bidco LLC/WH i3B Topco, LLC', 'Stratus Networks, Inc.', 'BIF IV Intrepid HoldCo, LLC')
    group by 1,2,3
),
bdc_market_provider_pcts as (
    select
        num.market,
        num.hocofinal,
        num.technology,
        num.numerator::float / nullif(denom.market_denominator::float,0.0) as bdc_pct_served
    from bdc_market_provider num
    left join bdc_market_denominator denom
        on num.market = denom.market
),
bdc_market_provider_pcts_cable as (
    select 
        market,
        listagg(hocofinal || ' (' || round(bdc_pct_served*100) || '%)', '; ') within group (order by market, bdc_pct_served desc) as bdc_1g_cable_providers
    from bdc_market_provider_pcts
    where technology = 'Cable'
    group by 1
),
bdc_market_provider_pcts_fiber as (
    select 
        market,
        listagg(hocofinal || ' (' || round(bdc_pct_served*100) || '%)', '; ') within group (order by market, bdc_pct_served desc) as bdc_1g_fiber_providers
    from bdc_market_provider_pcts
    where technology = 'Fiber'
    group by 1
),
-- BDC HS Counts
bdc_high_speed_counts_per_location as (
    select 
        location_id,
        census_block_code_2020,
        pct_in_footprint,
        market,
        bdc_to_passings,
        COUNT(DISTINCT case
            WHEN resi_tech_code IN ('40','50') 
                AND resi_down_speed >= 100
                AND resi_up_speed >= 20
                AND hocofinal not in ('WH i3B Bidco LLC/WH i3B Topco, LLC', 'Stratus Networks, Inc.', 'BIF IV Intrepid HoldCo, LLC')
            THEN hocofinal END) as num_hs_providers
    FROM bdc_by_provider
    GROUP BY 1,2,3,4,5
),
bdc_high_speed_counts_by_market as (
    select
        cb.market,
        sum(case when num_hs_providers = 0 or num_hs_providers is null then bdc_to_passings *  pct_in_footprint * 1.0 / nullif(market_denominator,0) else 0 end) as pct_0_1g_comp,
        sum(case when num_hs_providers = 1 then bdc_to_passings * pct_in_footprint * 1.0 / nullif(market_denominator,0) else 0 end) as pct_1_1g_comp,
        sum(case when num_hs_providers = 2 then bdc_to_passings * pct_in_footprint * 1.0 / nullif(market_denominator,0) else 0 end) as pct_2_1g_comp,
        sum(case when num_hs_providers >= 3 then bdc_to_passings * pct_in_footprint * 1.0 / nullif(market_denominator,0) else 0 end) as pct_3_plus_1g_comp,
        sum(num_hs_providers * bdc_to_passings * pct_in_footprint) * 1.0 / sum(nullif(case when num_hs_providers is not null then bdc_to_passings * pct_in_footprint end,0)) as avg_num_1g_providers
    FROM bdc_high_speed_counts_per_location cb
    LEFT JOIN bdc_market_denominator bdc
        on cb.market = bdc.market
    GROUP BY 1
),
-- BB Penetration
precisely_predicted_pen as (
    select
        cb.market,
        -- sum(unique_pbkeys) as precisely_passings,
        sum(predicted_penetration_12m * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as predicted_penetration_12m,
        sum(predicted_penetration_36m * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as predicted_penetration_36m,
        sum(predicted_penetration_60m * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as predicted_penetration_60m
    from base_cbs cb
    join {{source('project_tmob2514','i3_penetration_cbs_normal_10132025')}} pen
        on cb.census_block_code_2020 = pen.cb_id
    group by 1
    
),
precisely_predicted_pen_uplift as (
    select
        cb.market,
        -- sum(unique_pbkeys) as precisely_passings,
        sum(predicted_penetration_12m_uplift * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as predicted_penetration_12m_w_uplift,
        sum(predicted_penetration_36m_uplift * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as predicted_penetration_36m_w_uplift,
        sum(predicted_penetration_60m_uplift * pct_in_footprint * total_passings) / nullif(sum(pct_in_footprint * total_passings), 0) as predicted_penetration_60m_w_uplift
    from base_cbs cb
    join {{ref('i3_penetration_cbs_uplift')}} pen_2
        on cb.census_block_code_2020 = pen_2.cb_id
    group by 1
),
precisely_rollup as (
    select
        cb.market,
        prc.unit_buckets,
        sum(cb.pct_in_footprint * prc.address_count) as precisely_passings
    from base_cbs cb
    left join {{source('project_tmob2514','i3_intrepid_cb_address_view_residential')}} prc
        on cb.census_block_code_2020 = prc.census_block_code_2020
    group by 1,2
),
precisely_counts as (
    select
        market,
        sum(case when unit_buckets = '1 Unit' then precisely_passings else 0 end) as precisely_units_1,
        sum(case when unit_buckets = '2-4 Units' then precisely_passings else 0 end) as precisely_units_2_4,
        sum(case when unit_buckets = '5-19 Units' then precisely_passings else 0 end) as precisely_units_5_19,
        sum(case when unit_buckets = '20+ Units' then precisely_passings else 0 end) as precisely_units_20_plus
    from precisely_rollup prc
    group by 1
),

precisely_band_rollup as (
    select
        cb.market,
        prc2.unit_buckets,
        prc2.band_label,
        sum(cb.pct_in_footprint * prc2.address_count) as precisely_passings
    from base_cbs cb
    left join {{source('project_tmob2514','i3_intrepid_cb_address_view_residential')}} prc2
        on cb.census_block_code_2020 = prc2.census_block_code_2020
    group by 1, 2, 3
),
precisely_band_counts as(
    select
        market,
        sum(case when unit_buckets = '1 Unit' and band_label in( '01. <100ft', '02. 100ft-200ft', '03. 200ft-300ft', '04. 300ft-400ft', '05. 400ft-500ft') then precisely_passings else 0 end) as precisely_units_1_0_500ft,
        sum(case when unit_buckets = '2-4 Units' and band_label in( '01. <100ft', '02. 100ft-200ft', '03. 200ft-300ft', '04. 300ft-400ft', '05. 400ft-500ft') then precisely_passings else 0 end) as precisely_units_2_4_0_500ft,
        sum(case when unit_buckets = '5-19 Units' and band_label in( '01. <100ft', '02. 100ft-200ft', '03. 200ft-300ft', '04. 300ft-400ft', '05. 400ft-500ft') then precisely_passings else 0 end) as precisely_units_5_19_0_500ft,
        sum(case when unit_buckets = '20+ Units' and band_label in( '01. <100ft', '02. 100ft-200ft', '03. 200ft-300ft', '04. 300ft-400ft', '05. 400ft-500ft') then precisely_passings else  0 end) as precisely_units_20_plus_0_500ft
    from precisely_band_rollup prc2
    group by 1
)

-- Final Joins
select
    ------------ Market Designation(s) ----------------
    base.market,
    SPLIT_PART(base.market, ' || ', 1) as census_tract_code,
    SPLIT_PART(base.market, ' || ', 4) as designation,
    base.csd_name,
    base.county,
    base.metro_name,
    base.dma_name,
    SPLIT_PART(base.market, ' || ', 2) as state,
    -- mkt.state,
    -- mkt.cbg_morphology,
    morph.pct_rural,
    morph.pct_suburban,
    morph.pct_urban,
    morph.pct_dense_urban,
    --------------------------------
    base.cb_count,
    -- base.total_passings_in_market, -- Comment out
    base.total_passings_in_footprint,
    ---------------- Demographics -------------------
    acs.total_housing_units,
    acs.total_households,
    acs.total_population,
    coalesce(biz.smb_count, 0) as smb_count,
    -- ACS 2016 Reweighted to 2020 CBs
    acs16.total_housing_units_2016,
    acs16.total_households_2016,
    acs16.total_population_2016,
    -- Road Miles and Raod Mile Density
    rm.road_mile_density,
    rmm.rural_road_mile_density,
    rmm.suburban_road_mile_density,
    rmm.urban_road_mile_density,
    rmm.dense_urban_road_mile_density,
    -- SFUs/MDUs
    acs.units_1,
    acs.units_2_4,
    acs.units_5_19,
    acs.units_20_plus,
    acs.units_mobile_home,
    acs.units_other_boat_rv_van,
    -- HH Metrics
    acs.occupancy_rate,
    acs.seasonal_occupancy_rate,
    acs.pct_hhs_income_below_20k,
    acs.pct_rented,
    acs.median_home_value,
    acs.median_household_income,
    acs.avg_hh_size,
    acs.pct_households_with_any_broadband,
    -- Population Metrics
    acs.higher_ed_rate,
    acs.pct_hispanic_or_latino_origin,
    acs.median_age,
    acs.unemployment_rate,
    lang.pct_english_only_speakers,
    lang.pct_other_language_speakers,
    lang.pct_other_language_speakers_spanish,
    -- Age Brackets
    acs.pct_pop_age_0_14,
    acs.pct_pop_age_15_17,
    acs.pct_pop_age_18_29,
    acs.pct_pop_age_30_44,
    acs.pct_pop_age_45_64,
    acs.pct_pop_age_65_plus,
    ----------------- Competition -------------------
    hs.pct_0_1g_comp,
    hs.pct_1_1g_comp,
    hs.pct_2_1g_comp,
    hs.pct_3_plus_1g_comp,
    hs.avg_num_1g_providers,
    fiber.bdc_1g_fiber_providers as top_1g_fiber_providers,
    cable.bdc_1g_cable_providers as top_1g_cable_providers,
    -- base.ilec, --Ccomment out for state rollup
    -- Penetration Data
    'N/A' as precisely_passings,
    pen.predicted_penetration_12m,
    pen.predicted_penetration_36m,
    pen.predicted_penetration_60m,
    pen_2.predicted_penetration_12m_w_uplift,
    pen_2.predicted_penetration_36m_w_uplift,
    pen_2.predicted_penetration_60m_w_uplift,
    'N/A' as precisely_units_1,
    'N/A' as precisely_units_2_4,
    'N/A' as precisely_units_5_19,
    'N/A' as precisely_units_20_plus,
    'N/A' as precisely_units_1_0_500ft,
    'N/A' as precisely_units_2_4_0_500ft,
    'N/A' as precisely_units_5_19_0_500ft,
    'N/A' as precisely_units_20_plus_0_500ft,

    SPLIT_PART(base.market, ' || ', 3)::int as ami_tag,
    base.county_passings_less_5k_flag,
    base.dma_greater_denver_flag

from market_summary base
-- Demos
left join businesses_by_market biz using(market)
left join morphology morph using(market)
left join acs_metrics acs using(market)
left join acs_2016_onto_2020 acs16 using(market)
left join road_mile_density_market rm using(market)
left join road_mile_density_final_morphology rmm using(market)
left join language_speakers_market_level lang using(market)
-- Competition
left join bdc_high_speed_counts_by_market hs using(market)
left join bdc_market_provider_pcts_fiber fiber using(market)
left join bdc_market_provider_pcts_cable cable using(market)
-- BB Penetration
left join precisely_predicted_pen pen using(market)
left join precisely_predicted_pen_uplift pen_2 using(market)
left join precisely_counts prc using(market)
left join precisely_band_counts pbc using(market)