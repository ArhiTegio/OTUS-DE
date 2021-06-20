with surce_data as (
    select * from {{ ref('uber-raw-data-apr14_tb') }}
),

foil as (
    select *, dispatching_base_number as base from {{ ref('Uber-Jan-Feb-FOIL_tb') }}
),

final_base as (
    select        
        surce_data.Date_Time,
        surce_data.Lat,
        surce_data.Lon,
        foil.dispatching_base_number as base,
        foil.active_vehicles,
        foil.trips
    from surce_data
    left join foil using (base)
)

select * from final_base