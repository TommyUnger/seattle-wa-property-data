CREATE TABLE import.pierce_sale(
etn VARCHAR(11)
, parcel_count Integer
, parcel_number VARCHAR(10)
, sale_date Date
, sale_price Decimal
, deed_type VARCHAR(40)
, grantor VARCHAR(80)
, grantee VARCHAR(80)
, valid_invalid VARCHAR(7)
, confirmed_uncomfirmed VARCHAR(11)
, exclude_reason VARCHAR(30)
, improved_vacant VARCHAR(8)
);
