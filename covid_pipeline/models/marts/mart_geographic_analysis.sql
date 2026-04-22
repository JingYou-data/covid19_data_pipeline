with county_data as (
    select * from {{ ref('int_county_with_demographics') }}
),

final as (
    select
        county,
        county_fips,
        state,
        date_updated,
        covid_cases_per_100k,
        covid_hospital_admissions_per_100k,
        covid_inpatient_bed_utilization,
        covid_19_community_level,
        total_population,
        median_age,
        elderly_pct,
        poverty_pct,
        elderly_vulnerability_index,

        -- community level encoding for dashboard
        case covid_19_community_level
            when 'Low'    then 1
            when 'Medium' then 2
            when 'High'   then 3
        end as community_level_score,

        --  Custom Metric #1: Healthcare Stress Score
        -
        round(
            coalesce(covid_inpatient_bed_utilization, 0) *
            coalesce(covid_hospital_admissions_per_100k, 0),
        4) as healthcare_stress_score

    from county_data
    where covid_cases_per_100k is not null
)

select * from final