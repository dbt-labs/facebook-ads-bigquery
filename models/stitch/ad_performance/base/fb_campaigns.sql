select distinct
    id,
    name

from {{ var('campaigns_table') }}
