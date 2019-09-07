select 
lutype::SMALLINT lu_type
, luitem::SMALLINT lu_item
, ludescription descr
, row_number()over(partition by lutype ORDER BY luitem::SMALLINT) row_num
from import.king_county_lookup 
