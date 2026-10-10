-- For the featured hip/knee codes only (the CMS readmission measure is a hip/knee measure):
-- each hospital's median dollar rate next to its 30-day readmission rate and confidence interval.
select
    v.hospital_ccn,
    v.hospital_name,
    v.procedure_label,
    v.code_type,
    v.code,
    v.setting,
    v.billing_class,
    v.billing_class_assumed,
    v.median_rate,
    v.n_dollar_rates,
    v.file_last_updated_on,
    v.county,
    v.hospital_type,
    v.hospital_ownership,
    o.readmission_rate,
    o.readmission_lower,
    o.readmission_upper,
    o.readmission_denominator,
    o.readmission_vs_national,
    o.readmission_footnote,
    coalesce(o.readmission_reported, false) as readmission_reported,
    o.readmission_start_date,
    o.readmission_end_date,
    o.complication_rate
from {{ ref('gold_price_variation') }} v
left join {{ ref('stg_cms_outcomes') }} o on v.hospital_ccn = o.hospital_ccn
where v.procedure_label is not null and v.n_dollar_rates > 0
