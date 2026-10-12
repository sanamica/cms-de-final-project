{# Staging query for a TALL price CSV: one row per item and payer/plan, with up to N code columns
   (code|1 ... code|N). Each code becomes its own row; item_codes lists all of the item's codes.
   has_billing_class: whether the file has a billing_class column (facility / professional).
   Rows with no rate at all (for example a gross charge with no payer) are dropped. #}
{% macro stage_tall_csv(table_name, source_key, code_slots, has_billing_class=false) -%}
{%- set item_codes_expr -%}
concat_ws('|', array_sort(array_distinct(array(
{%- for i in code_slots %}
    case when nullif(trim(`code|{{ i }}`), '') is not null then concat(upper(`code|{{ i }}|type`), ':', `code|{{ i }}`) end{{ ',' if not loop.last }}
{%- endfor %}
))))
{%- endset -%}
with coded as (
    {%- for i in code_slots %}
    select
        description as item_description, modifiers, setting,
        {{ 'billing_class' if has_billing_class else 'cast(null as string)' }} as billing_class_raw,
        `standard_charge|gross` as gross_raw, `standard_charge|min` as min_raw, `standard_charge|max` as max_raw,
        payer_name, plan_name,
        `standard_charge|negotiated_dollar` as dollar_raw,
        `standard_charge|negotiated_percentage` as percentage_raw,
        `standard_charge|negotiated_algorithm` as algorithm_raw,
        `standard_charge|methodology` as methodology,
        {{ item_codes_expr }} as item_codes,
        _source_file, _run_id,
        `code|{{ i }}` as code, upper(`code|{{ i }}|type`) as code_type
    from {{ source('bronze', table_name) }}
    where nullif(trim(`code|{{ i }}`), '') is not null
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

{{ label_and_scope('coded') }}

select
    '{{ source_key }}' as source_key,
    item_description, item_codes, nullif(trim(modifiers), '') as modifiers, code, code_type, procedure_label,
    setting, billing_class_raw as billing_class, payer_name, plan_name,
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
where coalesce(nullif(trim(dollar_raw), ''), nullif(trim(percentage_raw), ''), nullif(trim(algorithm_raw), '')) is not null
{%- endmacro %}
