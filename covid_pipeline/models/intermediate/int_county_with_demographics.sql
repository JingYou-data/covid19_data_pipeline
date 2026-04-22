select
    c.county,
    c.county_fips,
    c.state,
    c.date_updated,
    c.covid_cases_per_100k,
    c.covid_hospital_admissions_per_100k,
    c.covid_inpatient_bed_utilization,
    c.covid_19_community_level,

    -- census demographics
    d.total_population,
    d.median_household_income,
    d.median_age,
    d.poverty_population,
    d.elderly_population,
    d.hispanic_population,
    d.black_population,

    -- derived metrics
    round(
        d.elderly_population / nullif(d.total_population, 0) * 100, 2
    ) as elderly_pct,

    round(
        d.poverty_population / nullif(d.total_population, 0) * 100, 2
    ) as poverty_pct,

    -- elderly vulnerability index
    round(
        (d.elderly_population / nullif(d.total_population, 0) * 100)
        * c.covid_cases_per_100k / 100, 2
    ) as elderly_vulnerability_index

from {{ ref('stg_county_covid') }} c
left join {{ ref('stg_census') }} d
    on c.county_fips = d.state_fips || d.county