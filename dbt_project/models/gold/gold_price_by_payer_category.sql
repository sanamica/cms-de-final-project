-- Negotiated dollar rates per hospital, code, setting, billing class AND payer category (commercial,
-- exchange, Medicare Advantage, Medicaid, other government). Each payer/plan is collapsed to one rate
-- first (the median of its dollar lines). Percentage, algorithm and placeholder lines are excluded
-- from the statistics but counted.
with comparable as (
    select * from {{ ref('silver_negotiated_rates') }}
    where {{ code_type_filter('code_type', 'comparable_code_types') }}
),

dollar as (
    select *,
        coalesce(billing_class, 'unspecified') as billing_class_group,
        case when rate_type = 'dollar' and not is_placeholder_rate then negotiated_dollar end as usable_rate
    from comparable
),

payer_plan as (
    select
        hospital_ccn, code_family, code, setting, billing_class_group, payer_category, payer_name, plan_name,
        percentile(usable_rate, 0.5) as payer_plan_rate
    from dollar
    group by hospital_ccn, code_family, code, setting, billing_class_group, payer_category, payer_name, plan_name
),

rate_stats as (
    select
        hospital_ccn, code_family, code, setting, billing_class_group, payer_category,
        count(payer_plan_rate)             as n_payer_plans,
        min(payer_plan_rate)               as min_rate,
        percentile(payer_plan_rate, 0.25)  as p25_rate,
        percentile(payer_plan_rate, 0.5)   as median_rate,
        percentile(payer_plan_rate, 0.75)  as p75_rate,
        max(payer_plan_rate)               as max_rate
    from payer_plan
    group by hospital_ccn, code_family, code, setting, billing_class_group, payer_category
),

line_counts as (
    select
        hospital_ccn, hospital_name, code_family, code, setting, billing_class_group, payer_category,
        max(procedure_label)                                       as procedure_label,
        max(item_description)                                      as item_description,
        max(file_last_updated_on)                                  as file_last_updated_on,
        sum(case when rate_type = 'percentage' then 1 else 0 end)  as n_percentage_lines,
        sum(case when rate_type = 'algorithm' then 1 else 0 end)   as n_algorithm_lines,
        sum(case when is_placeholder_rate then 1 else 0 end)       as n_placeholder_lines
    from dollar
    group by hospital_ccn, hospital_name, code_family, code, setting, billing_class_group, payer_category
)

select
    l.hospital_ccn, l.hospital_name, l.code_family, l.code, l.setting,
    l.billing_class_group                  as billing_class,
    l.payer_category,
    l.procedure_label, l.item_description, l.file_last_updated_on,
    coalesce(s.n_payer_plans, 0)           as n_payer_plans,
    s.min_rate, s.p25_rate, s.median_rate, s.p75_rate, s.max_rate,
    l.n_percentage_lines, l.n_algorithm_lines, l.n_placeholder_lines
from line_counts l
left join rate_stats s
    on  l.hospital_ccn        <=> s.hospital_ccn
    and l.code_family         <=> s.code_family
    and l.code                <=> s.code
    and l.setting             <=> s.setting
    and l.billing_class_group <=> s.billing_class_group
    and l.payer_category      <=> s.payer_category
