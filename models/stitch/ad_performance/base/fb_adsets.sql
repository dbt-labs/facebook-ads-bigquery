
-- TODO : use this distinct?

select distinct
    id,
    name

from {{ var('adsets_table') }}
