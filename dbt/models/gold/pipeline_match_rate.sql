-- Pipeline health: did every prediction for a past game find its box score?

select
    game_date,
    count(*)                                                     as predictions,
    count_if(outcome_status = 'evaluated')                       as evaluated,
    count_if(outcome_status = 'did_not_play')                    as did_not_play,
    count_if(outcome_status = 'no_box_score')                    as no_box_score,
    round(
        count_if(outcome_status in ('evaluated', 'did_not_play'))
        / nullif(count_if(outcome_status <> 'not_played_yet'), 0), 3
    )                                                            as match_rate
from {{ ref('fct_prediction_outcomes') }}
group by game_date