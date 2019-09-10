DROP TABLE IF EXISTS property.pierce_county_parcel_geo;
CREATE TABLE property.pierce_county_parcel_geo
AS
SELECT 
taxparceln::BIGINT pin	
, delivery_a address
, REGEXP_REPLACE(city_state, ', WA', '') city
, CASE WHEN zipcode ~* '[^0-9-]' THEN NULL ELSE left(regexp_replace(zipcode, '[-].*', ''), 5) END::INT zip_code	
, business_n business_name
, land_acres*43560.0 sqft_lot	
, land_value
, improvemen improvement_value
, use_code::SMALLINT use_code
, landuse_de::pierce_county_landuse_type land_use_type
, longitude lon
, latitude lat
, ST_TRANSFORM(geom, 4269) geom
, ST_Centroid(ST_TRANSFORM(geom, 4269)) geom_centroid
FROM import.pierce_county_parcel_geo
;

CREATE INDEX IDX_pierce_county_parcel_geo_pin
ON property.pierce_county_parcel_geo(pin)
;