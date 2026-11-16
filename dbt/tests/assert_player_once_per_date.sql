-- A player appearing twice on one date would duplicate every prediction joined to them.
select game_date, player_key, count(*) as rows
from {{ source('silver', 'player_box_scores') }}
group by all
having count(*) > 1