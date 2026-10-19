-- one row: the words joined in order_no order
select string_agg(word, ' ' order by order_no) as phrase
from {{ ref('stg_fragments') }}
