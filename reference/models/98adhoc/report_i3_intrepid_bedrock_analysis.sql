{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (tract_id)"
        ]
    )
}}

with cb_with_geom as (
    select 
        a.census_tract_code,
        a.census_block_code_2020,
        a.ami_tag,
        a.county_name,
        a.state,
        t.geom as cb_geom
    from {{ref('report_i3_intrepid_expansion_tracts')}} a
    inner join {{ref('report_predicted_cbs_in_expansion_geoms')}} b
        on a.census_block_code_2020 = b.census_block_code_2020
    left join {{source('tiger','tabblock')}} t
        on a.census_block_code_2020 = t.tabblock_id
),
cb_bedrock_analysis as (
    select 
        a.census_tract_code,
        a.census_block_code_2020,
        a.ami_tag,
        a.county_name,
        a.state,
        AVG(b.gridcode) as avg_bedrock_depth_ft
    from cb_with_geom a
    left join {{source('geology','usa_bedrock_depth_polygons_90m_20200723')}} b
        on ST_Intersects(a.cb_geom, b.geom)
    group by 1,2,3,4,5
),
cb_depth_buckets as (
    select 
        census_tract_code,
        census_block_code_2020,
        ami_tag,
        county_name,
        state,
        avg_bedrock_depth_ft,
        case 
            when avg_bedrock_depth_ft >= 0 and avg_bedrock_depth_ft < 1 then '0-1ft'
            when avg_bedrock_depth_ft >= 1 and avg_bedrock_depth_ft < 2 then '1-2ft'
            when avg_bedrock_depth_ft >= 2 and avg_bedrock_depth_ft < 3 then '2-3ft'
            when avg_bedrock_depth_ft >= 3 and avg_bedrock_depth_ft < 4 then '3-4ft'
            when avg_bedrock_depth_ft >= 4 and avg_bedrock_depth_ft < 5 then '4-5ft'
            when avg_bedrock_depth_ft >= 5 and avg_bedrock_depth_ft < 6 then '5-6ft'
            when avg_bedrock_depth_ft >= 6 and avg_bedrock_depth_ft < 7 then '6-7ft'
            when avg_bedrock_depth_ft >= 7 and avg_bedrock_depth_ft < 8 then '7-8ft'
            when avg_bedrock_depth_ft >= 8 and avg_bedrock_depth_ft < 9 then '8-9ft'
            when avg_bedrock_depth_ft >= 9 and avg_bedrock_depth_ft < 10 then '9-10ft'
            when avg_bedrock_depth_ft >= 10 then '10+ft'
            else 'unknown'
        end as depth_bucket
    from cb_bedrock_analysis
),
tract_totals as (
    select 
        census_tract_code,
        count(distinct census_block_code_2020) as total_cbs_in_tract
    from cb_depth_buckets
    group by 1
),
tract_depth_distribution as (
    select 
        cb.census_tract_code as tract_id,
        max(cb.ami_tag) as ami_tag,
        max(cb.county_name) as county_name,
        max(cb.state) as state,
        tt.total_cbs_in_tract,
        
        -- Calculate percentages for each depth bucket
        count(case when depth_bucket = '0-1ft' then 1 end)::float / tt.total_cbs_in_tract as pct_0_1ft,
        count(case when depth_bucket = '1-2ft' then 1 end)::float / tt.total_cbs_in_tract as pct_1_2ft,
        count(case when depth_bucket = '2-3ft' then 1 end)::float / tt.total_cbs_in_tract as pct_2_3ft,
        count(case when depth_bucket = '3-4ft' then 1 end)::float / tt.total_cbs_in_tract as pct_3_4ft,
        count(case when depth_bucket = '4-5ft' then 1 end)::float / tt.total_cbs_in_tract as pct_4_5ft,
        count(case when depth_bucket = '5-6ft' then 1 end)::float / tt.total_cbs_in_tract as pct_5_6ft,
        count(case when depth_bucket = '6-7ft' then 1 end)::float / tt.total_cbs_in_tract as pct_6_7ft,
        count(case when depth_bucket = '7-8ft' then 1 end)::float / tt.total_cbs_in_tract as pct_7_8ft,
        count(case when depth_bucket = '8-9ft' then 1 end)::float / tt.total_cbs_in_tract as pct_8_9ft,
        count(case when depth_bucket = '9-10ft' then 1 end)::float / tt.total_cbs_in_tract as pct_9_10ft,
        count(case when depth_bucket = '10+ft' then 1 end)::float / tt.total_cbs_in_tract as pct_10_plus_ft,
        count(case when depth_bucket = 'unknown' then 1 end)::float / tt.total_cbs_in_tract as pct_unknown
        
    from cb_depth_buckets cb
    join tract_totals tt on cb.census_tract_code = tt.census_tract_code
    group by 1, 5
)
select * from tract_depth_distribution
order by tract_id
