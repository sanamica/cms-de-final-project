-- One row per hospital in the project: the crosswalk (which price file belongs to which CCN)
-- enriched with CMS Hospital General Information, CMS outcome availability, and price file facts.
select
    x.hospital_ccn,
    x.hospital_name,
    x.source_key,
    h.hospital_name                                  as cms_hospital_name,
    h.city,
    h.county,
    h.state,
    h.zip_code,
    h.hospital_type,
    h.hospital_ownership,
    (h.hospital_ccn is not null)                     as in_cms_general_info,
    coalesce(o.readmission_reported, false)          as has_cms_readmission_data,
    (o.complication_rate is not null)                as has_cms_complication_data,
    f.last_updated_on                                as price_file_last_updated_on,
    f.hospital_name_in_file,
    f.file_version                                   as price_file_version
from {{ ref('hospital_crosswalk') }} x
left join {{ ref('stg_cms_hospitals') }} h on x.hospital_ccn = h.hospital_ccn
left join {{ ref('stg_cms_outcomes') }} o on x.hospital_ccn = o.hospital_ccn
left join {{ ref('stg_price_file_headers') }} f on x.source_key = f.source_key
