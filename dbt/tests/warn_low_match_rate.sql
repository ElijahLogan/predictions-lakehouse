{{ config(severity='warn') }}

-- Flag recent days where under 90% of predictions found a box score: usually name mismatches.
select game_date, match_rate
from {{ ref('pipeline_match_rate') }}
where game_date >= date_sub(current_date(), 14)
  and match_rate < 0.9