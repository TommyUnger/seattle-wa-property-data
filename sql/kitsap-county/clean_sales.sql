CREATE TABLE property.kitsap_county_sale
AS
SELECT pin
, address
, sqft_lot
, land_infl
, quality
, home_type
, year_built
, sqft_finished
, condition
, perc_finished
, sale_date
, sale_price
FROM import.kitsap_sale
