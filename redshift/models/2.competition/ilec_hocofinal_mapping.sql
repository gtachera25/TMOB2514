{{ 
    config(
        materialized='table'
    ) 
}}

select 'AT&T' as ilec, 'AT&T Inc.' as hocofinal
union all
select 'BRIGHTSPEED' as ilec, 'Connect Holding II LLC' as hocofinal
union all
select 'FRONTIER COMMUNICATIONS' as ilec, 'Frontier Communications Corporation' as hocofinal
union all
select 'VERIZON' as ilec, 'Verizon Communications Inc.' as hocofinal
union all
select 'GLASFORD TELEPHONE CO' as ilec, 'Glasford Telephone Company' as hocofinal
union all
select 'LUMEN' as ilec, 'Lumen Technologies, Inc.' as hocofinal

