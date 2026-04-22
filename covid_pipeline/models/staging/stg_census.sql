select
    name                                        as county_name,
    state                                       as state_fips,
    county,
    cast(b01003_001e as bigint)                 as total_population,
    cast(b19013_001e as bigint)                 as median_household_income,
    cast(b01002_001e as float)                  as median_age,
    cast(b02001_002e as bigint)                 as white_population,
    cast(b02001_003e as bigint)                 as black_population,
    cast(b03003_003e as bigint)                 as hispanic_population,
    cast(b17001_002e as bigint)                 as poverty_population,
    cast(b01001_006e as bigint)                 as elderly_population,

    case state
        when '01' then 'Alabama'
        when '02' then 'Alaska'
        when '04' then 'Arizona'
        when '05' then 'Arkansas'
        when '06' then 'California'
        when '08' then 'Colorado'
        when '09' then 'Connecticut'
        when '10' then 'Delaware'
        when '11' then 'District of Columbia'
        when '12' then 'Florida'
        when '13' then 'Georgia'
        when '15' then 'Hawaii'
        when '16' then 'Idaho'
        when '17' then 'Illinois'
        when '18' then 'Indiana'
        when '19' then 'Iowa'
        when '20' then 'Kansas'
        when '21' then 'Kentucky'
        when '22' then 'Louisiana'
        when '23' then 'Maine'
        when '24' then 'Maryland'
        when '25' then 'Massachusetts'
        when '26' then 'Michigan'
        when '27' then 'Minnesota'
        when '28' then 'Mississippi'
        when '29' then 'Missouri'
        when '30' then 'Montana'
        when '31' then 'Nebraska'
        when '32' then 'Nevada'
        when '33' then 'New Hampshire'
        when '34' then 'New Jersey'
        when '35' then 'New Mexico'
        when '36' then 'New York'
        when '37' then 'North Carolina'
        when '38' then 'North Dakota'
        when '39' then 'Ohio'
        when '40' then 'Oklahoma'
        when '41' then 'Oregon'
        when '42' then 'Pennsylvania'
        when '44' then 'Rhode Island'
        when '45' then 'South Carolina'
        when '46' then 'South Dakota'
        when '47' then 'Tennessee'
        when '48' then 'Texas'
        when '49' then 'Utah'
        when '50' then 'Vermont'
        when '51' then 'Virginia'
        when '53' then 'Washington'
        when '54' then 'West Virginia'
        when '55' then 'Wisconsin'
        when '56' then 'Wyoming'
        when '72' then 'Puerto Rico'
    end                                         as state_name

from {{ source('bronze', 'CENSUS_DATA') }}