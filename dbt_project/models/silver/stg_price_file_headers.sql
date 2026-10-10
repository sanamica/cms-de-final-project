-- One row per price file, from the metadata the bronze load kept in the <key>_header tables.
-- The date appears as 2025-10-01 in some files and 1/1/2026 or 01/01/2026 in others.
{% set keys = ['vumc_mrf', 'sthss_mrf', 'struth_mrf', 'wmc_mrf'] %}

with headers as (
    {%- for k in keys %}
    select '{{ k }}' as source_key, lower(trim(header_name)) as header_name, header_value
    from {{ source('bronze', k ~ '_header') }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

pivoted as (
    select
        source_key,
        max(case when header_name = 'last_updated_on' then trim(header_value) end) as last_updated_raw,
        max(case when header_name = 'hospital_name'   then trim(header_value) end) as hospital_name_in_file,
        max(case when header_name = 'version'         then trim(header_value) end) as file_version
    from headers
    group by source_key
)

select
    source_key,
    last_updated_raw,
    coalesce(
        cast(try_to_timestamp(last_updated_raw, 'yyyy-MM-dd') as date),
        cast(try_to_timestamp(last_updated_raw, 'M/d/yyyy') as date),
        cast(try_cast(last_updated_raw as timestamp) as date)
    ) as last_updated_on,
    hospital_name_in_file,
    file_version
from pivoted
