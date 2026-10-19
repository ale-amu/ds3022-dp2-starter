-- one row per fragment: order_no as an integer, word trimmed
-- also distinct helps makes it safe against a duplicate fragment 
select distinct
    cast(order_no as integer) as order_no,
    trim(word) as word
from {{ source('raw', 'fragments') }}
