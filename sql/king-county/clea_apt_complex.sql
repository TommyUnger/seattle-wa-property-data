DROP TABLE IF EXISTS property.king_county_apt_complex;
CREATE TABLE property.king_county_apt_complex
AS
SELECT 
major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, complex_descr complex_desc
, nbr_bldgs::SMALLINT nbr_buildings
, nbr_units::SMALLINT nbr_units
, avg_unit_size::SMALLINT avg_unit_size
, project_location::SMALLINT project_location
, project_appeal::SMALLINT project_appeal
, pcnt_with_view::SMALLINT pct_view
, constr_class::SMALLINT constr_class
, bldg_quality::SMALLINT bldg_quality
, condition::SMALLINT condition
, yr_built::SMALLINT year_built
, eff_yr::SMALLINT year_finished
, pcnt_complete::SMALLINT pct_complete
, CASE WHEN elevators = 'Y' THEN 1 ELSE 0 END::BOOL elevators
, laundry::SMALLINT laundry
, address::VARCHAR(150) address
FROM import.king_county_apt_complex
WHERE major <> 'Major'
;

CREATE INDEX IDX_king_county_apt_complex 
ON property.king_county_apt_complex(pin)
;