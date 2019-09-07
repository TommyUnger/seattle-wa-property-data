SELECT 
major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, bldg_nbr::SMALLINT bldg_nbr
, nbr_bldgs::SMALLINT nbr_buildings
, address::VARCHAR(200) address
, building_number::VARCHAR(200) building_number
, fraction::VARCHAR(200) fraction
, direction_prefix::VARCHAR(200) direction_prefix
, street_name::VARCHAR(200) street_name
, street_type::VARCHAR(200) street_type
, direction_suffix::VARCHAR(200) direction_suffix
, LEFT(regexp_replace(zip_code, '[^0-9]+', '0', 'g'), 5)::INT zip_code
, nbr_stories::FLOAT stories
, constr_class::SMALLINT constr_class
, bldg_quality::SMALLINT bldg_quality
, yr_built::SMALLINT year_built
, eff_yr::SMALLINT year_finished
, predominant_use::SMALLINT predominant_use
, shape::SMALLINT shape
, bldg_gross_sq_ft::INT sqft_bldg_gross
, bldg_net_sq_ft::INT sqft_bldg_net
, bldg_descr bldg_desc
, pcnt_complete::SMALLINT pct_complete
, CASE WHEN elevators = 'Y' THEN 1 ELSE 0 END::BOOL elevators
FROM import.king_county_comm_bldg
