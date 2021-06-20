with source_over as (
    select * from {{ ref('other-Skyline_B00111_tb') }}
),

final_over as (
    select        
        concat(source_over.Date_, source_over.Time_) as Date_Time,
        source_over.Street_Address,
        source_over.City_State
    from source_over
)

select * from final_over