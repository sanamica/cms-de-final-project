-- Hip/knee replacement outcomes per hospital from two CMS measure files. Values are text in
-- bronze; non-numeric scores such as 'Not Available' become NULL (flagged by *_reported).
{% set uhv = source('bronze', 'unplanned_hospital_visits') %}
{% set cad = source('bronze', 'complications_and_deaths') %}

with readmission as (
    select * from (
        select
            facility_id as hospital_ccn,
            try_cast(nullif(trim(score), '') as double)                                   as readmission_rate,
            try_cast(nullif(trim({{ col_or_null(uhv, 'lower_estimate') }}), '') as double) as readmission_lower,
            try_cast(nullif(trim({{ col_or_null(uhv, 'higher_estimate') }}), '') as double) as readmission_upper,
            try_cast(nullif(trim({{ col_or_null(uhv, 'denominator') }}), '') as double)    as readmission_denominator,
            {{ col_or_null(uhv, 'compared_to_national') }}                                as readmission_vs_national,
            {{ col_or_null(uhv, 'footnote') }}                                            as readmission_footnote,
            {{ col_or_null(uhv, 'start_date') }}                                          as readmission_start_date,
            {{ col_or_null(uhv, 'end_date') }}                                            as readmission_end_date,
            row_number() over (partition by facility_id order by {{ col_or_null(uhv, 'end_date') }} desc) as rn
        from {{ uhv }}
        where upper(measure_id) like '%READM%' and upper(measure_id) like '%HIP%KNEE%'
    ) t
    where rn = 1
),

complications as (
    select * from (
        select
            facility_id as hospital_ccn,
            try_cast(nullif(trim(score), '') as double)                                   as complication_rate,
            {{ col_or_null(cad, 'compared_to_national') }}                                as complication_vs_national,
            row_number() over (partition by facility_id order by {{ col_or_null(cad, 'end_date') }} desc) as rn
        from {{ cad }}
        where upper(measure_id) like '%COMP%' and upper(measure_id) like '%HIP%KNEE%'
    ) t
    where rn = 1
)

select
    r.hospital_ccn,
    r.readmission_rate, r.readmission_lower, r.readmission_upper, r.readmission_denominator,
    r.readmission_vs_national, r.readmission_footnote, r.readmission_start_date, r.readmission_end_date,
    (r.readmission_rate is not null)                  as readmission_reported,
    c.complication_rate, c.complication_vs_national
from readmission r
left join complications c on r.hospital_ccn = c.hospital_ccn
