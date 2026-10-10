-- Ascension Saint Thomas Rutherford: tall CSV (one row per item and payer/plan).
-- An item can carry up to two billing codes (code|1, code|2); each becomes its own row.
-- item_codes lists ALL the item's codes (including the hospital's own CDM code), which tells apart
-- different chargemaster lines that share one CPT code.
with coded as (
    select
        description as item_description, modifiers, setting,
        `standard_charge|gross` as gross_raw, `standard_charge|min` as min_raw, `standard_charge|max` as max_raw,
        payer_name, plan_name,
        `standard_charge|negotiated_dollar` as dollar_raw,
        `standard_charge|negotiated_percentage` as percentage_raw,
        `standard_charge|negotiated_algorithm` as algorithm_raw,
        `standard_charge|methodology` as methodology,
        concat_ws('|',
            case when nullif(trim(`code|1`), '') is not null then concat(upper(`code|1|type`), ':', `code|1`) end,
            case when nullif(trim(`code|2`), '') is not null then concat(upper(`code|2|type`), ':', `code|2`) end) as item_codes,
        _source_file, _run_id,
        `code|1` as code, upper(`code|1|type`) as code_type
    from {{ source('bronze', 'struth_mrf') }}
    where nullif(trim(`code|1`), '') is not null

    union all

    select
        description, modifiers, setting,
        `standard_charge|gross`, `standard_charge|min`, `standard_charge|max`,
        payer_name, plan_name,
        `standard_charge|negotiated_dollar`, `standard_charge|negotiated_percentage`,
        `standard_charge|negotiated_algorithm`, `standard_charge|methodology`,
        concat_ws('|',
            case when nullif(trim(`code|1`), '') is not null then concat(upper(`code|1|type`), ':', `code|1`) end,
            case when nullif(trim(`code|2`), '') is not null then concat(upper(`code|2|type`), ':', `code|2`) end),
        _source_file, _run_id,
        `code|2`, upper(`code|2|type`)
    from {{ source('bronze', 'struth_mrf') }}
    where nullif(trim(`code|2`), '') is not null
),

{{ label_and_scope('coded') }}

select
    'struth_mrf' as source_key,
    item_description, item_codes, nullif(trim(modifiers), '') as modifiers, code, code_type, procedure_label,
    setting, cast(null as string) as billing_class, payer_name, plan_name,
    try_cast(nullif(trim(dollar_raw), '') as double)     as negotiated_dollar,
    try_cast(nullif(trim(percentage_raw), '') as double) as negotiated_percentage,
    nullif(trim(algorithm_raw), '')                      as negotiated_algorithm,
    cast(null as double)                                 as estimated_amount,
    methodology,
    try_cast(nullif(trim(gross_raw), '') as double)      as gross_charge,
    try_cast(nullif(trim(min_raw), '') as double)        as min_charge,
    try_cast(nullif(trim(max_raw), '') as double)        as max_charge,
    _source_file, _run_id
from labeled
