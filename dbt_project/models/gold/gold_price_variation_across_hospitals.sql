-- How much does the same service (code + setting + billing class) differ between hospitals?
-- Uses each hospital's median dollar rate, so a hospital with many plans does not outweigh the others.
-- A hospital only counts if it has at least min_payer_plans_for_comparison payer/plans with a dollar
-- rate for that service; one or two plans say little about a hospital's prices.
select
    code_family,
    code,
    setting,
    billing_class,
    max(procedure_label)                                  as procedure_label,
    max(item_description)                                 as item_description,
    count(*)                                              as n_hospitals,
    {{ var('min_payer_plans_for_comparison', 5) }}        as min_payer_plans_required,
    min(file_last_updated_on)                             as oldest_file_date,
    max(file_last_updated_on)                             as newest_file_date,
    min(median_rate)                                      as lowest_hospital_median,
    percentile(median_rate, 0.5)                          as median_of_hospital_medians,
    max(median_rate)                                      as highest_hospital_median,
    max(median_rate) / nullif(min(median_rate), 0)        as hospital_spread_ratio
from {{ ref('gold_price_variation') }}
where n_dollar_rates >= {{ var('min_payer_plans_for_comparison', 5) }}
group by code_family, code, setting, billing_class
