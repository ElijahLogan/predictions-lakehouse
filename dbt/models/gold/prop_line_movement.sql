with versions as (
    select game_date, player_key, player_name, target, bookmaker, side,
           line, price, __START_AT as seen_at
    from {{ source('silver', 'player_prop_lines') }}
)
select
    game_date, player_key, any_value(player_name) as player_name, target, bookmaker,
    min_by(line,  seen_at) filter (where side = 'over')  as opening_line,
    max_by(line,  seen_at) filter (where side = 'over')  as latest_line,
    max_by(line,  seen_at) filter (where side = 'over')
      - min_by(line, seen_at) filter (where side = 'over') as line_move,
    min_by(price, seen_at) filter (where side = 'over')  as opening_over_price,
    max_by(price, seen_at) filter (where side = 'over')  as latest_over_price,
    max_by(price, seen_at) filter (where side = 'under') as latest_under_price,
    count(*) filter (where side = 'over')                as over_versions,
    min(seen_at) as first_seen_at,
    max(seen_at) as last_changed_at
from versions
group by game_date, player_key, target, bookmaker