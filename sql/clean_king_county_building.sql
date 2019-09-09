DROP TABLE IF EXISTS property.king_county_building;
CREATE TABLE property.king_county_building
AS
SELECT 
major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, bldg_nbr::SMALLINT nbr_buildings
, nbr_living_units::SMALLINT nbr_living_units
, address::VARCHAR(200) address
, building_number::VARCHAR(200) building_number
, fraction::VARCHAR(200) fraction
, direction_prefix::VARCHAR(200) direction_prefix
, street_name::VARCHAR(200) street_name
, street_type::VARCHAR(200) street_type
, direction_suffix::VARCHAR(200) direction_suffix
, LEFT(regexp_replace(zip_code, '[^0-9]+', '0', 'g'), 5)::INT zip_code
, stories::FLOAT stories
, bldg_grade::SMALLINT bldg_grade
, sq_ft_unfin_full::INT sqft_unfinished
, sq_ft_tot_living::INT sqft_total_living
, sq_ft_fin_basement::INT sqft_basement
, sq_ft_garage_attached::SMALLINT sqft_garage
, CASE WHEN view_utilization IN ('Y', 'y') THEN 1 ELSE 0 END::BOOL view_utilization
, bedrooms::SMALLINT bedrooms
, bath_half_count::FLOAT * 0.5 + bath3qtr_count::FLOAT * 0.75 + bath_full_count::FLOAT baths
, yr_built::SMALLINT year_built
, yr_renovated::SMALLINT year_renovated
, pcnt_complete::SMALLINT pct_complete
, obsolescence::SMALLINT pct_obsolete
, pcnt_net_condition::SMALLINT pct_net_condition
, brick_stone::SMALLINT pct_brick_stone
, condition::SMALLINT condition
, addnl_cost::INT addnl_cost
, fp_single_story::SMALLINT fp_single_story
, fp_multi_story::SMALLINT fp_multi_story
, fp_freestanding::SMALLINT fp_freestanding
, fp_additional::SMALLINT fp_additional
FROM import.king_county_building
WHERE major <> 'Major'
;

CREATE INDEX IDX_property_king_county_building_pin
ON property.king_county_building(pin)
;