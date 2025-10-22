{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}"
        ]
    )
}}

select
    census_block_code_2020,
    market_name,
    provider,
    distance_bucket,
    count(distinct business_source_id) as address_count
from {{ref('distinct_smb_locations_distance_to_fiber')}}
group by 1,2,3,4
order by 1,2,4,3