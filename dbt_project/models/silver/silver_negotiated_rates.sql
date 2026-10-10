-- One row per hospital, item code, setting, payer and plan, across all four price files.
-- Materialized as a TABLE (the other silver models are views) because parsing the 1.9 GB VUMC
-- JSON and unpivoting Williamson's wide columns is too slow to repeat on every downstream query.
{{ config(materialized='table') }}

-- DISTINCT removes lines that repeat exactly (same item, code, payer, plan, amount): they add no
-- information and would be counted twice in the gold statistics.
with unioned as (
    select distinct * from (
        select * from {{ ref('stg_vumc_rates') }}
        union all
        select * from {{ ref('stg_sthss_rates') }}
        union all
        select * from {{ ref('stg_struth_rates') }}
        union all
        select * from {{ ref('stg_wmc_rates') }}
    ) all_files
)

select
    u.source_key,
    x.hospital_ccn,
    x.hospital_name,
    u.code_type,
    u.code,
    u.procedure_label,
    (u.procedure_label is not null)                      as is_featured,
    u.item_description,
    u.item_codes,
    u.modifiers,
    lower(trim(u.setting))                               as setting,
    -- Files that do not label facility vs professional fees get the hospital's documented assumption
    -- from the crosswalk seed (blank = unknown, which stays NULL).
    coalesce(nullif(lower(trim(u.billing_class)), ''), nullif(lower(trim(x.assumed_billing_class)), '')) as billing_class,
    (nullif(trim(u.billing_class), '') is null and nullif(trim(x.assumed_billing_class), '') is not null) as billing_class_assumed,
    trim(u.payer_name)                                   as payer_name,
    trim(u.plan_name)                                    as plan_name,
    case
        when u.negotiated_dollar is not null     then 'dollar'
        when u.negotiated_percentage is not null then 'percentage'
        when u.negotiated_algorithm is not null  then 'algorithm'
        else 'none'
    end                                                  as rate_type,
    u.negotiated_dollar,
    u.negotiated_percentage,
    u.negotiated_algorithm,
    u.estimated_amount,
    u.methodology,
    u.gross_charge,
    u.min_charge,
    u.max_charge,
    (u.negotiated_dollar is not null and u.negotiated_dollar < {{ var('min_plausible_rate', 1) }}) as is_placeholder_rate,
    f.last_updated_on                                    as file_last_updated_on,
    u._source_file,
    u._run_id
from unioned u
join {{ ref('hospital_crosswalk') }} x on u.source_key = x.source_key
left join {{ ref('stg_price_file_headers') }} f on u.source_key = f.source_key
