-- Gold must have exactly one outcome per current prediction: none lost, none duplicated.
select p.n as predictions, o.n as outcomes
from (select count(*) as n from {{ ref('int_current_predictions') }}) p
cross join (select count(*) as n from {{ ref('fct_prediction_outcomes') }}) o
where p.n <> o.n