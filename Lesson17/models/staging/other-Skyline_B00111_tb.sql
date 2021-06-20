{{ config(materialized='table') }}

with source_data as (
    select * from {{ ref('other-Skyline_B00111') }}
),

renamed as (
    select 
        Date_,
        Time_,
        Street_Address,
        City_State,
        Castom1,
        Castom2
    from source_data
)

select * from renamed
