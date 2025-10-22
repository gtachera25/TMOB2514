{{ 
    config(
        materialized='table'
    ) 
}}


    select * from dblink('deltacity', $REDSHIFT$
                select
                 census_tract_code,
                 census_block_code_2020,
                 county_name,
                 state,
                 ami_tag
    from project_tmob2514.i3_intrepid_tracts a
    $REDSHIFT$) as t1 (
        census_tract_code varchar(256),
        census_block_code_2020 varchar(15),
        county_name text,
        state varchar(2),
        ami_tag int
    )

