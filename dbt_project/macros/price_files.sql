{# ---------------------------------------------------------------------------
   Helpers for turning the four hospital price files (bronze) into one standard shape.
   Every staging model returns exactly these columns, in this order:
     source_key, item_description, item_codes, modifiers, code, code_type, procedure_label, setting, billing_class,
     payer_name, plan_name, negotiated_dollar, negotiated_percentage, negotiated_algorithm,
     estimated_amount, methodology, gross_charge, min_charge, max_charge, _source_file, _run_id
   --------------------------------------------------------------------------- #}


{# The column if it exists in the relation, otherwise a typed NULL. Keeps models working if CMS
   adds or drops an optional column. #}
{% macro col_or_null(relation, column, as_type='string') -%}
    {%- set found = [] -%}
    {%- if execute -%}
        {%- for c in adapter.get_columns_in_relation(relation) -%}
            {%- if c.name | lower == column | lower -%}{%- set _ = found.append(c.name) -%}{%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if found -%}`{{ found[0] }}`{%- else -%}cast(null as {{ as_type }}){%- endif -%}
{%- endmacro %}


{# WHERE-style condition keeping only the configured billing code types. #}
{% macro code_type_filter(col, var_name='silver_code_types') -%}
    {%- set types = var(var_name, ['CPT', 'HCPCS', 'MS-DRG']) -%}
    {%- if types is string and types | lower == 'all' -%}
        1 = 1
    {%- else -%}
        upper({{ col }}) in ({% for t in types %}'{{ t | upper }}'{{ ',' if not loop.last }}{% endfor %})
    {%- endif -%}
{%- endmacro %}


{# CTE named `labeled`: adds the featured-procedure label (hip/knee) to a CTE that has `code` and
   `code_type`, applies the code type filter, and, when procedure_scope = 'featured', keeps only
   the labeled procedures. #}
{% macro label_and_scope(coded_cte) -%}
labeled as (
    select c.*, pc.procedure_label
    from {{ coded_cte }} c
    left join {{ ref('procedure_codes') }} pc
        on c.code = pc.code and upper(c.code_type) = upper(pc.code_type)
    where {{ code_type_filter('c.code_type') }}
    {%- if var('procedure_scope', 'all') == 'featured' %}
      and pc.procedure_label is not null
    {%- endif %}
)
{%- endmacro %}


{# DDL schema for the CMS price transparency JSON items. Every scalar is read as a string and
   cast later, so one oddly typed value can never null out a whole item. #}
{% macro json_price_schema() -%}
description string, code_information array<struct<code:string,type:string>>, standard_charges array<struct<setting:string,billing_class:string,gross_charge:string,discounted_cash:string,minimum:string,maximum:string,payers_information:array<struct<payer_name:string,plan_name:string,standard_charge_dollar:string,standard_charge_percentage:string,standard_charge_algorithm:string,estimated_amount:string,methodology:string>>>>
{%- endmacro %}


{# Staging query for a JSON price file whose bronze table has one `raw_json` item per row. #}
{% macro stage_json_price_file(source_name, table_name, source_key) -%}
with raw as (
    select b.raw_json, b._source_file, b._run_id
    from {{ source(source_name, table_name) }} b
    {%- if var('procedure_scope', 'all') == 'featured' %}
    where exists (
        select 1 from {{ ref('procedure_codes') }} pf
        where instr(b.raw_json, concat('"', pf.code, '"')) > 0
    )
    {%- endif %}
),

parsed as (
    select from_json(raw_json, '{{ json_price_schema() }}') as j, _source_file, _run_id
    from raw
),

coded as (
    select
        j,
        concat_ws('|', array_sort(array_distinct(transform(j.code_information, x -> concat(upper(x.type), ':', x.code))))) as item_codes,
        ci.code as code, upper(ci.type) as code_type, _source_file, _run_id
    from parsed
    lateral view explode(array_distinct(j.code_information)) lv1 as ci
),

{{ label_and_scope('coded') }},

rates as (
    select
        l.code, l.code_type, l.procedure_label, l.item_codes, l.j.description as item_description,
        l._source_file, l._run_id,
        sc.setting, sc.billing_class, sc.gross_charge, sc.minimum as min_charge, sc.maximum as max_charge,
        p.payer_name, p.plan_name, p.standard_charge_dollar, p.standard_charge_percentage,
        p.standard_charge_algorithm, p.estimated_amount, p.methodology
    from labeled l
    lateral view explode(l.j.standard_charges) lv2 as sc
    lateral view explode(sc.payers_information) lv3 as p
)

select
    '{{ source_key }}' as source_key,
    item_description, item_codes, cast(null as string) as modifiers, code, code_type, procedure_label,
    setting, billing_class, payer_name, plan_name,
    try_cast(nullif(trim(standard_charge_dollar), '') as double)     as negotiated_dollar,
    try_cast(nullif(trim(standard_charge_percentage), '') as double) as negotiated_percentage,
    nullif(trim(standard_charge_algorithm), '')                      as negotiated_algorithm,
    try_cast(nullif(trim(estimated_amount), '') as double)           as estimated_amount,
    methodology,
    try_cast(nullif(trim(gross_charge), '') as double)               as gross_charge,
    try_cast(nullif(trim(min_charge), '') as double)                 as min_charge,
    try_cast(nullif(trim(max_charge), '') as double)                 as max_charge,
    _source_file, _run_id
from rates
{%- endmacro %}


{# For a WIDE price CSV, where each payer/plan has its own columns named
   standard_charge|<payer>|<plan>|negotiated_dollar  (and _percentage, _algorithm, |methodology).
   Returns a stack() expression that turns those columns into one row per payer/plan with the
   columns payer_name, plan_name, dollar_raw, percentage_raw, algorithm_raw, methodology_raw. #}
{% macro payer_rate_stack(relation) -%}
    {%- set suffixes = {
        'negotiated_dollar': 'dollar', 'negotiated_percentage': 'percentage',
        'negotiated_algorithm': 'algorithm', 'methodology': 'methodology'} -%}
    {%- set groups = {} -%}
    {%- if execute -%}
        {%- for c in adapter.get_columns_in_relation(relation) -%}
            {%- set name = c.name -%}
            {%- if name.startswith('standard_charge|') -%}
                {%- for suffix, key in suffixes.items() -%}
                    {%- if name.endswith('|' ~ suffix) -%}
                        {%- set mid = name[('standard_charge|' | length):(name | length) - (suffix | length) - 1] -%}
                        {%- if '|' in mid -%}
                            {%- if mid not in groups -%}{%- set _ = groups.update({mid: {}}) -%}{%- endif -%}
                            {%- set _ = groups[mid].update({key: name}) -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if groups | length == 0 -%}
        stack(1, cast(null as string), cast(null as string), cast(null as string), cast(null as string), cast(null as string), cast(null as string))
            as (payer_name, plan_name, dollar_raw, percentage_raw, algorithm_raw, methodology_raw)
    {%- else -%}
        stack({{ groups | length }}
        {%- for mid in groups | sort -%}
            {%- set parts = mid.split('|') -%}
            , '{{ parts[0] | replace("\\", "\\\\") | replace("'", "\\'") }}'
            , '{{ parts[1:] | join('|') | replace("\\", "\\\\") | replace("'", "\\'") }}'
            {%- for key in ['dollar', 'percentage', 'algorithm', 'methodology'] -%}
                , {% if key in groups[mid] %}`{{ groups[mid][key] | replace('`', '``') }}`{% else %}cast(null as string){% endif %}
            {%- endfor -%}
        {%- endfor -%}
        ) as (payer_name, plan_name, dollar_raw, percentage_raw, algorithm_raw, methodology_raw)
    {%- endif -%}
{%- endmacro %}
