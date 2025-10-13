{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}"
        ]
    )
}}

select *
from {{ source('tmob2514','i3_intrepid_base_location_counts_in_fpt_cb_residential') }}