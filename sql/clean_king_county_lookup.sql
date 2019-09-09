DROP TABLE IF EXISTS property.king_county_lookup;

CREATE TABLE property.king_county_lookup
AS
SELECT
lutype::SMALLINT lu_type
, luitem::SMALLINT lu_item
, ludescription descr
, row_number()over(partition by lutype ORDER BY luitem::SMALLINT) row_num
from import.king_county_lookup 
WHERE lutype <> 'LUType'
;

CREATE INDEX IDX_king_county_lookup
ON property.king_county_lookup(lu_type, lu_item)
;