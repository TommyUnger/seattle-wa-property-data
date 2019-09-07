SELECT 
major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, unit_type::SMALLINT unit_type
, bldg_nbr::VARCHAR(64) bldg_nbr
, unit_nbr::VARCHAR(64) unit_nbr
, CASE WHEN floor_nbr = 'B' THEN 0 ELSE floor_nbr::SMALLINT END floor_nbr
, CASE WHEN top_floor = 'Y' THEN 1 ELSE 0 END::SMALLINT top_floor
, address::VARCHAR(640) address
, regexp_replace(building_number, ',', '')::VARCHAR(64) building_number
, fraction::VARCHAR(6) fraction
, direction_prefix::VARCHAR(6) direction_prefix
, street_name::VARCHAR(640) street_name
, street_type::VARCHAR(64) street_type
, direction_suffix::VARCHAR(6) direction_suffix
, LEFT(regexp_replace(zip_code, '[^0-9]+', '0', 'g'), 5)::INT zip_code
, footage::INT sqft_total_living
, unit_of_measure::SMALLINT unit_of_measure
, CASE WHEN nbr_bedrooms='S' THEN 0.5 ELSE nbr_bedrooms::FLOAT END bedrooms
, bath_half_count::FLOAT * 0.5 + bath3qtr_count::FLOAT * 0.75 + bath_full_count::FLOAT baths
, yr_built::SMALLINT year_built
, pcnt_ownership::FLOAT pct_ownership_complete
, condition::SMALLINT condition
, grade::SMALLINT bldg_grade
, view_mountain::SMALLINT view_mountain
, view_lake_river::SMALLINT view_lake_river
, view_city_territorial::SMALLINT view_city_territorial
, view_puget_sound::SMALLINT view_puget_sound
, view_lake_wa_samm::SMALLINT view_lake_wa_samm
, unit_quality::SMALLINT unit_quality
, pkg_garage::SMALLINT pkg_garage
, pkg_basement::SMALLINT pkg_basement
FROM import.king_county_condo_unit