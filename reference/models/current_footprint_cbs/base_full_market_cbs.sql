{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (census_block_code_2020)",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}

--runtime: 43.57s, 2537668 rows
select 
    tabblock_id as census_block_code_2020,
  case statefp
    when '04' then 'AZ'
    when '06' then 'CA'
    when '08' then 'CO'
    when '17' then 'IL'
    when '19' then 'IA'
    when '25' then 'MA'
    when '26' then 'MI'
    when '27' then 'MN'
    when '29' then 'MO'
    when '42' then 'PA'
    when '44' then 'RI'
    else 'Unknown'
  end as state,
    geom
  from {{ source('tiger','tabblock') }}
  where statefp in ('04', '06', '08', '17', '19', '25', '26', '27', '29', '42', '44')


