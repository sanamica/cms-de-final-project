-- A negotiated dollar rate can never be negative. Any row returned is a failure.
select * from {{ ref('silver_negotiated_rates') }}
where negotiated_dollar < 0
