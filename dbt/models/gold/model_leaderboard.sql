-- Accuracy per model and stat, over predictions for players who played.

select
    model_name,
    model_family,
    model_variant,
    target,
    count(*)                                                     as evaluated_predictions,
    round(avg(abs_error), 3)                                     as mae,
    round(sqrt(avg(error * error)), 3)                           as rmse,
    round(avg(error), 3)                                         as bias,
    round(avg(case when not is_default then abs_error end), 3)   as mae_excluding_defaults,
    round(avg(baseline_abs_error), 3)                            as baseline_mae,
    round(1 - avg(abs_error) / nullif(avg(baseline_abs_error), 0), 3) as skill_vs_baseline,
    round(avg(case when is_default then 1.0 else 0.0 end), 3)    as default_share
from {{ ref('fct_prediction_outcomes') }}
where outcome_status = 'evaluated'
group by all