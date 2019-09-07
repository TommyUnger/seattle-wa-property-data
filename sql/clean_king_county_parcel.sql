SELECT 
major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, prop_name::VARCHAR(200) prop_name
, plat_name::VARCHAR(200) plat_name
, plat_lot::VARCHAR(200) plat_lot
, plat_block::VARCHAR(200) plat_block
, range::SMALLINT prop_range
, township::VARCHAR(200) township
, section::SMALLINT section
, quarter_section::VARCHAR(6) quarter_section
, prop_type::CHAR(1) prop_type
, area::SMALLINT area
, sub_area::SMALLINT sub_area
, spec_area::SMALLINT spec_area
, spec_sub_area::SMALLINT spec_sub_area
, district_name::VARCHAR(200) district_name
, levy_code::SMALLINT levy_code
, current_zoning::VARCHAR(16) current_zoning
, hbuas_if_vacant::SMALLINT hbuas_if_vacant
, hbuas_improved::SMALLINT hbuas_improved
, present_use::SMALLINT present_use
, sq_ft_lot::INT sqft_lot
, mt_rainier::SMALLINT view_mt_rainier
, olympics::SMALLINT view_olympics
, cascades::SMALLINT view_cascades
, territorial::SMALLINT view_territorial
, seattle_skyline::SMALLINT view_seattle_skyline
, puget_sound::SMALLINT view_puget_sound
, lake_washington::SMALLINT view_lake_washington
, lake_sammamish::SMALLINT view_lake_sammamish
, small_lake_river_creek::SMALLINT view_small_lake_river_creek
, other_view::SMALLINT view_other
, water_system::SMALLINT water_system
, sewer_system::SMALLINT sewer_system
, access::SMALLINT prop_access
, topography::SMALLINT topography
, street_surface::SMALLINT street_surface
, restrictive_sz_shape::SMALLINT restrictive_sz_shape
, inadequate_parking::SMALLINT inadequate_parking
, pcnt_unusable::SMALLINT pct_unusable
, CASE WHEN unbuildable = 'True' THEN 1 ELSE 0 END::BOOL unbuildable
, wfnt_location::SMALLINT wfnt_location
, wfnt_footage::INT wfnt_footage
, wfnt_bank::SMALLINT wfnt_bank
, wfnt_poor_quality::SMALLINT wfnt_poor_quality
, wfnt_restricted_access::SMALLINT wfnt_restricted_access
, CASE WHEN wfnt_access_rights = 'Y' THEN 1 ELSE 0 END::BOOL wfnt_access_rights
, CASE WHEN wfnt_proximity_influence = 'Y' THEN 1 ELSE 0 END::BOOL wfnt_proximity_influence
, tideland_shoreland::SMALLINT tideland_shoreland
, lot_depth_factor::SMALLINT lot_depth_factor
, traffic_noise::SMALLINT traffic_noise
, airport_noise::SMALLINT airport_noise
, CASE WHEN power_lines = 'Y' THEN 1 ELSE 0 END::BOOL power_lines
, CASE WHEN other_nuisances = 'Y' THEN 1 ELSE 0 END::BOOL other_nuisances
, nbr_bldg_sites::SMALLINT nbr_bldg_sites
, contamination::SMALLINT contamination
, CASE WHEN dnrlease = 'Y' THEN 1 ELSE 0 END::BOOL dnrlease
, CASE WHEN adjacent_golf_fairway = 'Y' THEN 1 ELSE 0 END::BOOL adjacent_golf_fairway
, CASE WHEN adjacent_greenbelt = 'Y' THEN 1 ELSE 0 END::BOOL adjacent_greenbelt
, historic_site::SMALLINT historic_site
, current_use_designation::SMALLINT current_use_designation
, CASE WHEN native_growth_prot_esmt = 'Y' THEN 1 ELSE 0 END::BOOL native_growth_prot_esmt
, CASE WHEN easements = 'Y' THEN 1 ELSE 0 END::BOOL easements
, CASE WHEN other_designation = 'Y' THEN 1 ELSE 0 END::BOOL other_designation
, CASE WHEN deed_restrictions = 'Y' THEN 1 ELSE 0 END::BOOL deed_restrictions
, CASE WHEN development_rights_purch = 'Y' THEN 1 ELSE 0 END::BOOL development_rights_purch
, CASE WHEN other_problems = 'Y' THEN 1 ELSE 0 END::BOOL other_problems
, CASE WHEN water_problems = 'Y' THEN 1 ELSE 0 END::BOOL water_problems
, CASE WHEN transp_concurrency = 'Y' THEN 1 ELSE 0 END::BOOL transp_concurrency
FROM import.king_county_parcel