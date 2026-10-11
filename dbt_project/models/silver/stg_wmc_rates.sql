-- Williamson Medical Center: wide CSV. Each payer/plan has its own columns, so the columns are
-- unpivoted into rows (payer_rate_stack builds the stack() from the table's column names).
{% set wide = source('bronze', 'wmc_mrf') %}

with coded as (
    {%- for i in [1, 2, 3, 4] %}
    select b.*, b.`code|{{ i }}` as code, upper(b.`code|{{ i }}|type`) as code_type
    from {{ wide }} b
    where nullif(trim(b.`code|{{ i }}`), '') is not null
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

{{ label_and_scope('coded') }},

long as (
    select l.*, {{ payer_rate_stack(wide) }}
    from labeled l
)

select
    'wmc_mrf' as source_key,
    description as item_description,
        concat_ws('|', array_sort(array_distinct(array(
        {%- for i in [1, 2, 3, 4] %}
        case when nullif(trim(`code|{{ i }}`), '') is not null then concat(upper(`code|{{ i }}|type`), ':', `code|{{ i }}`) end{{ ',' if not loop.last }}
        {%- endfor %}
    )))) as item_codes,
    nullif(trim(modifiers), '') as modifiers,
    code, code_type, procedure_label,
    setting, billing_class, payer_name, plan_name,
    try_cast(nullif(trim(dollar_raw), '') as double)     as negotiated_dollar,
    try_cast(nullif(trim(percentage_raw), '') as double) as negotiated_percentage,
    nullif(trim(algorithm_raw), '')                      as negotiated_algorithm,
    cast(null as double)                                 as estimated_amount,
    nullif(trim(methodology_raw), '')                    as methodology,
    try_cast(nullif(trim(`standard_charge|gross`), '') as double) as gross_charge,
    try_cast(nullif(trim(`standard_charge|min`), '') as double)   as min_charge,
    try_cast(nullif(trim(`standard_charge|max`), '') as double)   as max_charge,
    _source_file, _run_id
from long
where coalesce(nullif(trim(dollar_raw), ''), nullif(trim(percentage_raw), ''), nullif(trim(algorithm_raw), '')) is not null
