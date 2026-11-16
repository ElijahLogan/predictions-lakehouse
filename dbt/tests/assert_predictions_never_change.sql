-- A key with more than one version means a later copy of a prediction disagreed with the first.
select game_date, player_key, model_name, target, count(*) as versions
from {{ source('silver', 'player_predictions') }}
group by all
having count(*) > 1