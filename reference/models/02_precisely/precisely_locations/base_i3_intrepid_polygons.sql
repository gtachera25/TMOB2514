{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add column cb_pkey serial primary key"
        ]
    )
}}

select *
from {{ ref('map_cbs_in_footprint') }}
