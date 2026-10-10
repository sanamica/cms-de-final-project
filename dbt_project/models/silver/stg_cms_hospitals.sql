{% set hgi = source('bronze', 'hospital_general_information') %}
select
    facility_id                                   as hospital_ccn,
    facility_name                                 as hospital_name,
    {{ col_or_null(hgi, 'citytown') }}            as city,
    {{ col_or_null(hgi, 'countyparish') }}        as county,
    state,
    {{ col_or_null(hgi, 'zip_code') }}            as zip_code,
    {{ col_or_null(hgi, 'hospital_type') }}       as hospital_type,
    {{ col_or_null(hgi, 'hospital_ownership') }}  as hospital_ownership
from {{ hgi }}
