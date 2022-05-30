--no_cache
select *
, split_part(lower(message_concat), ':', 1) as sender
, answer_title as response_title
from operations.fct_ada_msg