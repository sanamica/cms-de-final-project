-- Negotiated-price spread per hospital, code, setting and billing class (facility and professional
-- fees are never pooled). DOLLAR rates drive every statistic; percentage, algorithm and placeholder
-- rates are excluded but counted so the exclusion is visible.
--
-- Each payer/plan is first collapsed to ONE rate (the median of its dollar lines), so a payer that a
-- file lists on several lines does not weigh more than a payer listed once.
--   n_dollar_rates = payer/plan combinations with a dollar rate
--   n_dollar_lines = the underlying dollar lines in the file
--   n_percentage_rates, n_algorithm_rates, n_placeholder_rates = excluded lines
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
        hospital_ccn, code_family, code, setting, billing_class_group, payer_name, plan_name,
        percentile(usable_rate, 0.5) as payer_plan_rate,
        count(usable_rate)           as n_lines
    from dollar
    group by hospital_ccn, code_family, code, setting, billing_class_group, payer_name, plan_name
),

rate_stats as (
    select
        hospital_ccn, code_family, code, setting, billing_class_group,
        count(payer_plan_rate)                                                     as n_dollar_rates,
        sum(n_lines)                                                               as n_dollar_lines,
        count(distinct case when payer_plan_rate is not null then payer_name end)  as n_payers,
        min(payer_plan_rate)                                                       as min_rate,
        percentile(payer_plan_rate, 0.25)                                          as p25_rate,
        percentile(payer_plan_rate, 0.5)                                           as median_rate,
        percentile(payer_plan_rate, 0.75)                                          as p75_rate,
        max(payer_plan_rate)                                                       as max_rate
    from payer_plan
    group by hospital_ccn, code_family, code, setting, billing_class_group
),

line_counts as (
    select
        hospital_ccn, hospital_name, code_family, code, setting, billing_class_group,
        max(procedure_label)                                       as procedure_label,
        max(item_description)                                      as item_description,
        max(file_last_updated_on)                                  as file_last_updated_on,
        bool_or(billing_class_assumed)                             as billing_class_assumed,
        sum(case when rate_type = 'percentage' then 1 else 0 end)  as n_percentage_rates,
        sum(case when rate_type = 'algorithm' then 1 else 0 end)   as n_algorithm_rates,
        sum(case when is_placeholder_rate then 1 else 0 end)       as n_placeholder_rates
    from dollar
    group by hospital_ccn, hospital_name, code_family, code, setting, billing_class_group
),

agg as (
    select
        l.hospital_ccn, l.hospital_name, l.code_family, l.code, l.setting,
        l.billing_class_group                  as billing_class,
        l.billing_class_assumed,
        l.procedure_label, l.item_description, l.file_last_updated_on,
        coalesce(s.n_dollar_rates, 0)          as n_dollar_rates,
        coalesce(s.n_dollar_lines, 0)          as n_dollar_lines,
        coalesce(s.n_payers, 0)                as n_payers,
        s.min_rate, s.p25_rate, s.median_rate, s.p75_rate, s.max_rate,
        s.max_rate / nullif(s.min_rate, 0)     as spread_ratio,
        l.n_percentage_rates, l.n_algorithm_rates, l.n_placeholder_rates
    from line_counts l
    left join rate_stats s
        on  l.hospital_ccn        <=> s.hospital_ccn
        and l.code_family           <=> s.code_family
        and l.code                <=> s.code
        and l.setting             <=> s.setting
        and l.billing_class_group <=> s.billing_class_group
)

select a.*, d.county, d.hospital_type, d.hospital_ownership
from agg a
left join {{ ref('dim_hospital') }} d on a.hospital_ccn = d.hospital_ccn
