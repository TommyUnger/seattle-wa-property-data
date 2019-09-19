DROP TABLE IF EXISTS property.snohomish_county_sale;
CREATE TABLE property.snohomish_county_sale
AS
SELECT 
lrsn::BIGINT lrsn
, "parcel id"::BIGINT pin
, "sd nbr"::FLOAT::SMALLINT sd_num
, nbhd::FLOAT::INT neighborhood_id
, "prop class"::SMALLINT property_class
, "situs address"::VARCHAR(128) address
, grantor::VARCHAR(128) seller_name
, "sale date"::DATE sale_date
, "sale price"::FLOAT::INT sale_price
, UPPER("deed type")::snohomish_deed_type_typs deed_type
, UPPER("sale qual code")::snohomish_sale_qual_code_typs sale_quality_code
, "total land size"::FLOAT*43560 sqft_lot
, "imp value"::FLOAT::INT improvement_value
, "mkt land value"::FLOAT::INT land_value
, UPPER("imp type")::snohomish_home_type_typs home_type
, grade::FLOAT::SMALLINT building_grade
, "yr blt"::FLOAT::SMALLINT year_built
, "eff yr blt"::FLOAT::SMALLINT year_built_effective
, UPPER("hse type desc")::snohomish_building_type_type house_type
, bedrooms::FLOAT::SMALLINT bedrooms
, "total sq_ft"::FLOAT::INT sqft_total
FROM import.snohomish_sale_combined
WHERE lrsn NOT IN ('LRSN', 'lrsn')
AND "sd nbr" NOT IN ('01/0')
;