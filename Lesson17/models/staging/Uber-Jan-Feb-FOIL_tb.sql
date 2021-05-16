{{ config(materialized='table') }}

with source_data as (
    select * from {{ ref('Uber-Jan-Feb-FOIL') }}
),

renamed as (
    select 
        dispatching_base_number,
        date_,
        active_vehicles,
        trips
    from source_data
)

select * from renamed