-- Negotiated-price spread per hospital, code, setting and billing class (facility and professional
-- fees are never mixed). DOLLAR rates drive every statistic;
-- percentage, algorithm and placeholder rates are excluded but counted so the exclusion is visible.
with comparable as (
    select * from {{ ref('silver_negotiated_rates') }}
    where {{ code_type_filter('code_type', 'comparable_code_types') }}
),

dollar as (
    select *, coalesce(billing_class, 'unspecified') as billing_class_group,
           case when rate_type = 'dollar' and not is_placeholder_rate then negotiated_dollar end as usable_rate
    from comparable
),

agg as (
select
    hospital_ccn,
    hospital_name,
    code_type,
    code,
    setting,
    billing_class_group                                               as billing_class,
    bool_or(billing_class_assumed)                                    as billing_class_assumed,
    max(procedure_label)                                              as procedure_label,
    max(item_description)                                             as item_description,
    max(file_last_updated_on)                                         as file_last_updated_on,
    count(usable_rate)                                                as n_dollar_rates,
    count(distinct case when usable_rate is not null then payer_name end) as n_payers,
    min(usable_rate)                                                  as min_rate,
    percentile(usable_rate, 0.25)                                     as p25_rate,
    percentile(usable_rate, 0.5)                                      as median_rate,
    percentile(usable_rate, 0.75)                                     as p75_rate,
    max(usable_rate)                                                  as max_rate,
    max(usable_rate) / nullif(min(usable_rate), 0)                    as spread_ratio,
    sum(case when rate_type = 'percentage' then 1 else 0 end)         as n_percentage_rates,
    sum(case when rate_type = 'algorithm' then 1 else 0 end)          as n_algorithm_rates,
    sum(case when is_placeholder_rate then 1 else 0 end)              as n_placeholder_rates
from dollar
group by hospital_ccn, hospital_name, code_type, code, setting, billing_class_group
)

select a.*, d.county, d.hospital_type, d.hospital_ownership
from agg a
left join {{ ref('dim_hospital') }} d on a.hospital_ccn = d.hospital_ccn
