{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{ this }} owner to {{ this.schema }}",
            "alter table {{ this }} add primary key (business_source_id)",
            "create index on {{ this }} using gist(geom)"
        ]
    )
}}

select
    distinct business_source_id,
    geom
from {{source('infousa_businesses_2025','businesses')}}
where emps < 25
    and st_isvalid(geom)
    and geom is not null
