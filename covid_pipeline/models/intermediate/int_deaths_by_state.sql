select
    state,
    week_ending_date,
    mmwr_week,
    year,
    covid_19_deaths,
    total_deaths,
    pneumonia_deaths,
    influenza_deaths,
    percent_of_expected_deaths,

    -- case fatality rate 新冠死亡率
    round(
        covid_19_deaths / nullif(total_deaths, 0) * 100, 2
    ) as covid_pct_of_total_deaths,

    -- pneumonia combo rate 脑炎死亡率
    round(
        pneumonia_deaths / nullif(total_deaths, 0) * 100, 2
    ) as pneumonia_pct_of_total_deaths
    
-- exclude the United States aggregate data to focus on state-level analysis 
from {{ ref('stg_covid_deaths') }}
where state != 'United States'