{{ config(materialized='table') }}

with source_data as (
    select * from {{ ref('uber-raw-data-apr14') }}
),

renamed as (
    select 
        Date_Time,
        Lat,
        Lon,
        Base
    from source_data
)

select * from renamed