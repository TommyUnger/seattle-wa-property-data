DROP TABLE IF EXISTS property.king_county_condo_complex;
CREATE TABLE property.king_county_condo_complex
AS
SELECT 
major::INT major
, complex_type::SMALLINT complex_type
, complex_descr::VARCHAR(200) complex_desc
, nbr_bldgs::SMALLINT nbr_buildings
, nbr_stories::SMALLINT stories
, nbr_units::SMALLINT nbr_units
, avg_unit_size::FLOAT avg_unit_size
, land_per_unit::FLOAT land_per_unit
, project_location::SMALLINT project_location
, project_appeal::SMALLINT project_appeal
, pcnt_with_view::SMALLINT pct_with_view
, constr_class::SMALLINT constr_class
, bldg_quality::SMALLINT bldg_quality
, condition::SMALLINT condition
, yr_built::SMALLINT year_built
, eff_yr::SMALLINT year_finished
, pcnt_complete::SMALLINT pct_complete
, CASE WHEN elevators = 'Y' THEN 1 ELSE 0 END::BOOL elevators
, CASE WHEN apt_conversion = 'Y' THEN 1 ELSE 0 END::BOOL apt_conversion
, condo_land_type::SMALLINT condo_land_type
, address::VARCHAR(200) address
, building_number::VARCHAR(200) building_number
, fraction::VARCHAR(200) fraction
, direction_prefix::VARCHAR(200) direction_prefix
, street_name::VARCHAR(200) street_name
, street_type::VARCHAR(200) street_type
, direction_suffix::VARCHAR(200) direction_suffix
, LEFT(regexp_replace(zip_code, '[^0-9]+', '0', 'g'), 5)::INT zip_code
FROM import.king_county_condo_complex
WHERE major <> 'Major'
;

CREATE INDEX IDX_property_king_county_condo_complex_pin
ON property.king_county_condo_complex(pin)
;
