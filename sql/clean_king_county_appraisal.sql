select 
major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, roll_yr::SMALLINT appr_year
, land_val::INT land_val
, imps_val::INT imp_val
, new_dollars::INT new_dollars
, select_method::SMALLINT select_method
, select_reason::SMALLINT select_reason
, updated_by::CHAR(4) updated_by
, post_status::SMALLINT post_status
FROM import.king_county_appraisal