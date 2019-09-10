DROP TABLE IF EXISTS property.snohomish_county_parcel_geo;
CREATE TABLE property.snohomish_county_parcel_geo
AS
SELECT 
parcel_id::BIGINT pin	
, source::SMALLINT source
, gis_sq_ft sqft_lot
, usecode::snohomish_county_use_type use_code
, mkimp improvement_value
, mklnd land_value
, replace(situsline1, 'UNKNOWN', '') address
, replace(situshouse, 'UNKNOWN', '') house_number
, replace(situsprefx, 'UNKNOWN', '') street_prefix
, replace(situsstrt, 'UNKNOWN', '') street_name
, replace(situsttyp, 'UNKNOWN', '') street_type
, replace(situspostd, 'UNKNOWN', '') street_suffix
, replace(situscity, 'UNKNOWN', '') city
, situszip zip_code
, ownername owner_name
, ownercity owner_city
, ownerstate owner_state
, ST_X(ST_Centroid(ST_MakeValid(ST_TRANSFORM(geom, 4269)))) lon
, ST_Y(ST_Centroid(ST_MakeValid(ST_TRANSFORM(geom, 4269)))) lat
, ST_TRANSFORM(geom, 4269) geom
, ST_Centroid(ST_MakeValid(ST_TRANSFORM(geom, 4269))) geom_centroid
FROM import.snohomish_county_parcel_geo
;

CREATE INDEX IDX_snohomish_county_parcel_geo_pin
ON property.snohomish_county_parcel_geo(pin)
;
