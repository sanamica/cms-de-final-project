-- Ascension Saint Thomas Rutherford: tall CSV with two code columns and no billing_class column.
{{ stage_tall_csv('struth_mrf', 'struth_mrf', [1, 2], has_billing_class=false) }}