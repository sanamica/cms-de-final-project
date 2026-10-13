-- One row per payer/plan name seen in the price files, with a payer category.
-- Rules live in seeds/payer_category_rules.csv: each is a regular expression matched against the
-- lower-cased payer and plan name (punctuation turned into spaces); the rule with the lowest priority
-- number wins. A name that matches no rule defaults to 'commercial' and is flagged
-- (payer_category_is_default), so the share of guessed names can be reviewed.
with payers as (
    select distinct trim(payer_name) as payer_name, trim(plan_name) as plan_name from {{ ref('stg_vumc_rates') }}
    union
    select distinct trim(payer_name), trim(plan_name) from {{ ref('stg_sthss_rates') }}
    union
    select distinct trim(payer_name), trim(plan_name) from {{ ref('stg_struth_rates') }}
    union
    select distinct trim(payer_name), trim(plan_name) from {{ ref('stg_wmc_rates') }}
),

normalized as (
    select
        payer_name, plan_name,
        trim(regexp_replace(lower(concat_ws(' ', payer_name, plan_name)), '[^a-z0-9]+', ' ')) as match_text
    from payers
),

matches as (
    select
        n.payer_name, n.plan_name, r.category, r.pattern,
        row_number() over (
            partition by n.payer_name, n.plan_name
            order by cast(r.priority as int), r.pattern
        ) as rn
    from normalized n
    join {{ ref('payer_category_rules') }} r on n.match_text rlike r.pattern
)

select
    n.payer_name,
    n.plan_name,
    coalesce(m.category, 'commercial')  as payer_category,
    (m.category is null)                as payer_category_is_default,
    m.pattern                           as matched_pattern
from normalized n
left join matches m
    on  n.payer_name <=> m.payer_name
    and n.plan_name  <=> m.plan_name
    and m.rn = 1
