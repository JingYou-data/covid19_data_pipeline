select
    state,
    cast(week_ending_date as date)                      as week_ending_date,
    mmwr_week,
    year,
    -- try_cast is used to handle any non-numeric values that may be present in the source data, 
    -- converting them to null if they cannot be cast to bigint
    try_cast(covid_19_deaths as bigint)                 as covid_19_deaths, 
    try_cast(total_deaths as bigint)                    as total_deaths,
    try_cast(pneumonia_deaths as bigint)                as pneumonia_deaths,
    try_cast(influenza_deaths as bigint)                as influenza_deaths,
    try_cast(percent_of_expected_deaths as float)       as percent_of_expected_deaths

from {{ source('bronze', 'COVID_DEATHS_DATA') }}
where state is not null
    and "GROUP" = 'By Week'
qualify row_number() over (
    partition by state, week_ending_date
    order by year desc, mmwr_week desc
) = 1