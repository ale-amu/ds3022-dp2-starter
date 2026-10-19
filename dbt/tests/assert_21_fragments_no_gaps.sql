-- fails (returns a row) unless there are exactly 21 fragments numbered 1 to 21 with no gaps
select
    count(*) as fragment_count,
    min(order_no) as min_order_no,
    max(order_no) as max_order_no,
    count(distinct order_no) as distinct_order_no
from {{ ref('stg_fragments') }}
having not (
    count(*) = {{ var('expected_fragments') }}
    and count(distinct order_no) = {{ var('expected_fragments') }}
    and min(order_no) = 1
    and max(order_no) = {{ var('expected_fragments') }}
)
