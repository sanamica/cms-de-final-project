-- Internal consistency of the gold spread statistics. Any row returned is a failure.
select * from {{ ref('gold_price_variation') }}
where n_dollar_rates > 0
  and (min_rate > median_rate or median_rate > max_rate or min_rate > max_rate)
