-- Question 2: within one hospital, how far below (or above) commercial rates do the other payer
-- categories pay for the same service? One row per hospital, code, setting and billing class.
-- Ratios are the category's median divided by the commercial median (NULL when either is missing).
select
    hospital_ccn,
    hospital_name,
    code_family,
    code,
    setting,
    billing_class,
    max(procedure_label)                                                          as procedure_label,
    max(file_last_updated_on)                                                     as file_last_updated_on,
    max(case when payer_category = 'commercial'         then median_rate end)     as commercial_median,
    max(case when payer_category = 'commercial'         then n_payer_plans end)   as commercial_n,
    max(case when payer_category = 'exchange'           then median_rate end)     as exchange_median,
    max(case when payer_category = 'exchange'           then n_payer_plans end)   as exchange_n,
    max(case when payer_category = 'medicare_advantage' then median_rate end)     as medicare_advantage_median,
    max(case when payer_category = 'medicare_advantage' then n_payer_plans end)   as medicare_advantage_n,
    max(case when payer_category = 'medicaid'           then median_rate end)     as medicaid_median,
    max(case when payer_category = 'medicaid'           then n_payer_plans end)   as medicaid_n,
    max(case when payer_category = 'exchange'           then median_rate end)
        / nullif(max(case when payer_category = 'commercial' then median_rate end), 0)           as exchange_to_commercial,
    max(case when payer_category = 'medicare_advantage' then median_rate end)
        / nullif(max(case when payer_category = 'commercial' then median_rate end), 0)           as medicare_advantage_to_commercial,
    max(case when payer_category = 'medicaid'           then median_rate end)
        / nullif(max(case when payer_category = 'commercial' then median_rate end), 0)           as medicaid_to_commercial
from {{ ref('gold_price_by_payer_category') }}
group by hospital_ccn, hospital_name, code_family, code, setting, billing_class
