select
    county,
    county_fips,
    state,
    cast(county_population as bigint)                       as county_population,
    health_service_area,
    cast(covid_cases_per_100k as float)                     as covid_cases_per_100k,
    cast(covid_hospital_admissions_per_100k as float)       as covid_hospital_admissions_per_100k,
    cast(covid_inpatient_bed_utilization as float)          as covid_inpatient_bed_utilization,
    covid_19_community_level,
    cast(date_updated as date)                              as date_updated

from {{ source('bronze', 'COUNTY_COVID_DATA') }}
where county_fips is not null