with census as (
    select * from {{ ref('stg_census') }}
),

deaths as (
    select * from {{ ref('int_deaths_by_state') }}
),

state_census as (
    select
        state_name,
        sum(total_population)           as total_population,
        avg(median_household_income)    as avg_median_income,
        avg(median_age)                 as avg_median_age,
        sum(elderly_population)         as total_elderly,
        sum(poverty_population)         as total_poverty,
        sum(black_population)           as total_black,
        sum(hispanic_population)        as total_hispanic,

        round(
            sum(elderly_population) / nullif(sum(total_population), 0) * 100, 2
        ) as elderly_pct,

        round(
            sum(poverty_population) / nullif(sum(total_population), 0) * 100, 2
        ) as poverty_pct

    from census
    group by state_name
),

joined as (
    select
        d.state,
        d.week_ending_date,
        d.covid_19_deaths,
        d.total_deaths,
        d.percent_of_expected_deaths,
        d.covid_pct_of_total_deaths,

        s.total_population,
        s.avg_median_income,
        s.avg_median_age,
        s.elderly_pct,
        s.poverty_pct,

        round(
            s.elderly_pct * d.covid_pct_of_total_deaths / 100, 2
        ) as elderly_vulnerability_index,

        round(
            s.poverty_pct * d.percent_of_expected_deaths / 100, 2
        ) as socioeconomic_impact_score

    from deaths d
    left join state_census s
        on d.state = s.state_name
)

select * from joined