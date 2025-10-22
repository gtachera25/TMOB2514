{{ config(
    materialized = 'table',
    post_hook = [
        "ALTER TABLE {{ this }} OWNER TO {{ this.schema }}"
    ]
) }}

-- got 28 rows

-- Pull minimal mapping from Redshift via DBLink: CBG code -> morphology
with cw_morph as (
    select *
    from dblink('deltacity', $REDSHIFT$
        select
            census_block_group_code,
            cbg_morphology
        from report_market.market_crosswalk_latest mkt
        group by 1,2
    $REDSHIFT$)
    as t(
        census_block_group_code varchar(256),
        cbg_morphology varchar(256))
),
-- Local CBG geometry with CBG code + state FIPS, used for spatial split and state tagging
cbg as (
    select
        bg.bg_id as census_block_group_code,
        bg.statefp as statefp,
        bg.geom as geom
    from {{ source('tiger', 'bg') }} bg
),
-- State lookup for 2-letter USPS code
state_lu as (
    select
        s.statefp as statefp,
        s.stusps as state
    from {{ source('tiger', 'state') }} s
),
-- Split fiber by CBG; compute miles per CBG segment
fiber_cbg_splits as (
    select
        f.id,
        case 
            when f.owner in ('UC2B', 'i3', 'Consolidated', 'circle fiber','BBN', 'Stratus Networks') then 'WH i3B Bidco LLC/WH i3B Topco, LLC'
            else 'BIF IV Intrepid HoldCo, LLC'
        end as provider,
        f.category,
        case 
            when f.market = 'Colorado; BroomfieldCO_boundary' then 'broomfield,co'
            when f.market = 'Colorado; PuebloCO_boundary' then 'pueblo,co'
            when f.market = 'Colorado; NorthglennCO_boundary' then 'northglenn,co'
            when f.market = 'Minnesota; EdenPrairieMN_boundary' then 'edenprairie,mn'
            when f.market = 'Colorado; LouisvilleLafayetteCO_boundary' then 'louisville lafayette,co'
            when f.market = 'Minnesota; BloomingtonMN_boundary' then 'bloomington,mn'
            when f.market = 'Minnesota; StCloudMN_boundary' then 'stcloud,mn'
            when f.market = 'Cherry Creek' then 'cherrycreek,co'
            when f.market = 'Minnesota; WoodburyMN_boundary' then 'woodbury,mn'
            when f.market = 'Minnesota; MinnetonkaMN_boundary' then 'minnetonka,mn'
            when f.market = 'Colorado; SuperiorCO_boundary' then 'superior,co'
            when f.market = 'Thornton, CO' then 'thornton,co'
            when f.market = 'Greenwood Village' then 'greenwood village,co'
            when f.market = 'Colorado; WestminsterExpansionCO_boundary' then 'westminster,co'
            when f.market = 'Colorado; WestminsterCO_boundary' then 'westminster,co'
            when f.market = 'Colorado; LittletonCO_boundary' then 'littleton,co'
            when f.region = 'Rhode Island' then f.market
            else f.region 
        end as market_name,
        f.status,
        c.census_block_group_code,
        c.statefp,
        -- length in miles of the clipped segment
        (ST_Length( ST_Intersection(f.geom, c.geom)::geography ) / 1609.34)::numeric as fiber_miles_in_cbg
        -- If you want QC geometry, uncomment:
        -- , ST_Intersection(f.geom, c.geom) as geom_split
    from {{ ref('base_i3_and_intrepid_consolidated') }} f
    join cbg c
      on ST_Intersects(f.geom, c.geom)
),
-- Attach morphology and state to each split
fiber_cbg_with_attrs as (
    select
        x.provider,
        s.state,
        x.market_name,
        x.category,
        x.status,
        sum(x.fiber_miles_in_cbg) as total_fiber_miles,
        sum(case when m.cbg_morphology = 'Rural' then x.fiber_miles_in_cbg else 0 end) as rural_fiber_miles,
        sum(case when m.cbg_morphology = 'Suburban' then x.fiber_miles_in_cbg else 0 end) as suburban_fiber_miles,
        sum(case when m.cbg_morphology = 'Urban' then x.fiber_miles_in_cbg else 0 end) as urban_fiber_miles,
        sum(case when m.cbg_morphology = 'Dense Urban' then x.fiber_miles_in_cbg else 0 end) as dense_urban_fiber_miles
    from fiber_cbg_splits x
    left join cw_morph m
      on x.census_block_group_code = m.census_block_group_code
    left join state_lu s
      on x.statefp = s.statefp
    group by 1,2,3,4,5
)
select * from fiber_cbg_with_attrs