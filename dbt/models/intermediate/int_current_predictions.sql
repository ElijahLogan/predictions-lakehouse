-- The current version of every prediction, with its model's baseline for that day and stat.

select
    p.game_date,
    p.player_key,
    p.player_name,
    p.model_name,
    p.model_family,
    p.model_variant,
    p.target,
    p.predicted_value,
    p.__START_AT                         as first_seen_at,
    p.was_swapped,
    b.baseline_value,
    p.predicted_value = b.baseline_value as is_default
from {{ source('silver', 'player_predictions') }} p
left join {{ source('silver', 'model_baselines') }} b
    on  b.game_date  = p.game_date
    and b.model_name = p.model_name
    and b.target     = p.target
where p.__END_AT is null