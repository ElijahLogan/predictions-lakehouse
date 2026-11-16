-- Box scores reshaped to one row per game date, player, and stat: the same shape as predictions.

with box_scores as (
    select game_date, player_key, player_name, player_team, did_play, minutes_played,
           cast(points as double)         as points,
           cast(assists as double)        as assists,
           cast(total_rebounds as double) as total_rebounds
    from {{ source('silver', 'player_box_scores') }}
)

select *
from box_scores
unpivot (actual_value for target in (points, assists, total_rebounds))