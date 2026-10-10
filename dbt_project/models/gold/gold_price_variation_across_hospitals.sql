-- How much does the same service (code + setting + billing class) differ between hospitals? Uses each
-- hospital's median dollar rate, so a hospital with many plans does not outweigh the others.
select
    code_type,
    code,
    setting,
    billing_class,
    max(procedure_label)                                  as procedure_label,
    max(item_description)                                 as item_description,
    count(*)                                              as n_hospitals,
    min(file_last_updated_on)                             as oldest_file_date,
    max(file_last_updated_on)                             as newest_file_date,
    min(median_rate)                                      as lowest_hospital_median,
    percentile(median_rate, 0.5)                          as median_of_hospital_medians,
    max(median_rate)                                      as highest_hospital_median,
    max(median_rate) / nullif(min(median_rate), 0)        as hospital_spread_ratio
from {{ ref('gold_price_variation') }}
where n_dollar_rates > 0
group by code_type, code, setting, billing_class
