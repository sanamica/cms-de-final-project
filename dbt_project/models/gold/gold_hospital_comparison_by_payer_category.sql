-- Question 1, made fair: how much does the same service differ between hospitals WITHIN a payer
-- category? Compare commercial with commercial, not a hospital's blend of Medicaid and commercial.
-- A hospital counts only with at least min_payer_plans_per_category payer/plans in that category.
select
    code_family,
    code,
    setting,
    billing_class,
    payer_category,
    max(procedure_label)                                  as procedure_label,
    max(item_description)                                 as item_description,
    count(*)                                              as n_hospitals,
    {{ var('min_payer_plans_per_category', 3) }}          as min_payer_plans_required,
    min(file_last_updated_on)                             as oldest_file_date,
    max(file_last_updated_on)                             as newest_file_date,
    min(median_rate)                                      as lowest_hospital_median,
    percentile(median_rate, 0.5)                          as median_of_hospital_medians,
    max(median_rate)                                      as highest_hospital_median,
    max(median_rate) / nullif(min(median_rate), 0)        as hospital_spread_ratio
from {{ ref('gold_price_by_payer_category') }}
where n_payer_plans >= {{ var('min_payer_plans_per_category', 3) }}
group by code_family, code, setting, billing_class, payer_category
