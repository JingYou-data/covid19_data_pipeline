with county_data as (
    select * from {{ ref('int_county_with_demographics') }}
),

deaths_data as (
    select * from {{ ref('int_deaths_by_state') }}
),

-- 7-day rolling average（窗口函数，按county按时间）
rolling_avg as (
    select
        state,
        date_updated,
        avg(covid_cases_per_100k) over (
            partition by state
            order by date_updated
            rows between 6 preceding and current row
        ) as rolling_7day_avg_cases_per_100k
    from county_data
    qualify row_number() over (
        partition by state, date_updated order by date_updated
    ) = 1
),

-- state level aggregation from county data
state_summary as (
    select
        state,
        date_updated,
        avg(covid_cases_per_100k)               as avg_cases_per_100k,
        avg(covid_hospital_admissions_per_100k) as avg_hospital_admissions_per_100k,
        avg(covid_inpatient_bed_utilization)    as avg_bed_utilization,
        count(distinct county_fips)             as county_count
    from county_data
    group by state, date_updated
),

final as (
    select
        d.state,
        d.week_ending_date,
        d.covid_19_deaths,
        d.total_deaths,
        d.covid_pct_of_total_deaths,
        d.percent_of_expected_deaths,
        d.pneumonia_pct_of_total_deaths,

        s.avg_cases_per_100k,
        s.avg_hospital_admissions_per_100k,
        s.avg_bed_utilization,
        s.county_count,

        r.rolling_7day_avg_cases_per_100k,

        --  Custom Metric #4: Excess Death Rate Trend
        
        case
            when d.percent_of_expected_deaths >= 150 then 'Severe Excess'
            when d.percent_of_expected_deaths >= 120 then 'High Excess'
            when d.percent_of_expected_deaths >= 100 then 'Above Expected'
            else 'Within Expected'
        end as excess_death_tier,

        d.percent_of_expected_deaths - 100 as excess_death_pct

    from deaths_data d
    left join state_summary s
        on d.state = s.state
        and date_trunc('week', d.week_ending_date) = date_trunc('week', s.date_updated)
    left join rolling_avg r
        on d.state = r.state
        and date_trunc('week', d.week_ending_date) = date_trunc('week', r.date_updated)
)

select * from final