-- Every current prediction next to what actually happened.

with predictions as (
    select * from {{ ref('int_current_predictions') }}
),

actuals as (
    select * from {{ ref('int_actuals_long') }}
)

select
    p.*,
    a.player_team,
    a.did_play,
    a.minutes_played,
    a.actual_value,
    case
        when a.player_key is not null and a.did_play then 'evaluated'
        when a.player_key is not null                then 'did_not_play'
        when p.game_date >= current_date()           then 'not_played_yet'
        else 'no_box_score'
    end                                                               as outcome_status,
    case when a.did_play then p.predicted_value - a.actual_value end      as error,
    case when a.did_play then abs(p.predicted_value - a.actual_value) end as abs_error,
    case when a.did_play then abs(p.baseline_value - a.actual_value) end  as baseline_abs_error
from predictions p
left join actuals a
    on  a.game_date  = p.game_date
    and a.player_key = p.player_key
    and a.target     = p.target