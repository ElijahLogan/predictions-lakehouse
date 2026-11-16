-- Daily accuracy per model and stat, with a rolling 7-game-day average to show drift.

with daily as (
    select game_date, model_name, target,
           count(*)       as predictions,
           sum(abs_error) as total_abs_error
    from {{ ref('fct_prediction_outcomes') }}
    where outcome_status = 'evaluated'
    group by all
)

select
    game_date,
    model_name,
    target,
    predictions,
    round(total_abs_error / predictions, 3) as mae,
    round(sum(total_abs_error) over last_7 / sum(predictions) over last_7, 3) as rolling_7_game_day_mae
from daily
window last_7 as (partition by model_name, target order by game_date rows between 6 preceding and current row)