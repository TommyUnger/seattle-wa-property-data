DROP TABLE IF EXISTS property.kitsap_county_parcel_geo;
CREATE TABLE property.kitsap_county_parcel_geo
AS
SELECT 
rp_acct_id::BIGINT pin	
, sid::kitsap_site_type site_type
, geo::SMALLINT geo
, ptn::SMALLINT partial_num
, full_addr	address
, city	city
, zip_code	zip_code
, unit_type::kitsap_unit_type	unit_type
, use_class::kitsap_use_code_type	use_code
, house_no	house_number
, prefix	street_prefix
, streetname	street_name
, streettype	street_type
, suffix	street_suffix
, shape_area sqft_lot
, ST_X(ST_TRANSFORM(a.geom, 4269)) lon
, ST_Y(ST_TRANSFORM(a.geom, 4269)) lat
, ST_TRANSFORM(g.geom, 4269) geom
, ST_TRANSFORM(a.geom, 4269) geom_centroid
FROM import.kitsap_county_parcel_geo g
JOIN import.kitsap_county_parcel_addr a ON a.ld_acct_id = g.rp_acct_id
WHERE g.rp_acct_id > 0
;

CREATE INDEX IDX_kitsap_county_parcel_geo_pin
ON property.kitsap_county_parcel_geo(pin)
;
