DROP TABLE IF EXISTS property.king_county_parcel_geo;
CREATE TABLE property.king_county_parcel_geo
AS
select 
regexp_replace(major, '[^0-9]+', '0', 'g')::INT major
, regexp_replace(minor, '[^0-9]+', '0', 'g')::SMALLINT minor
, regexp_replace(major, '[^0-9]+', '0', 'g')::BIGINT*10000 + regexp_replace(minor, '[^0-9]+', '0', 'g')::BIGINT pin
, sitetype::CHAR(2) site_type
, siteid::INT site_id
, ctyname city
, postalctyn city_postal
, levy_juris city_levy
, addr_hn address_num
, addr_pd street_dir1
, addr_sn street_name
, addr_st street_type
, addr_sd street_dir2
, unit_num
, bldg_num
, addr_full
, zip5::INT zip_code
, apprlndval::INT appr_land
, appr_impr::INT appr_improvements
, prop_name property_name
, proptype::CHAR(1) property_type
, plat_name
, lotsqft::INT sqft_lot
, accnt_num tax_account_num
, condositus condo_address
, kctp_state taxpayer_state
, preuse_cod::SMALLINT use_code
, preuse_des use_name
, comments
, lat
, lon
, ST_TRANSFORM(geom, 4269) geom
, ST_Centroid(ST_TRANSFORM(geom, 4269)) geom_centroid
FROM import.king_county_parcel_geo
;

CREATE INDEX IDX_property_king_county_parcel_geo_pin
ON property.king_county_parcel_geo(pin)
;
