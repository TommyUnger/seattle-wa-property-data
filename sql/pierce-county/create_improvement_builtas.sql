CREATE TABLE import.pierce_improvement_builtas(
parcel_number VARCHAR(10)
, building_id VARCHAR(5)
, built_as_number Integer
, built_as_id Integer
, built_as_description VARCHAR(255)
, built_as_square_feet Integer
, hvac Integer
, hvac_description VARCHAR(30)
, exterior VARCHAR(25)
, interior VARCHAR(15)
, stories Decimal
, story_height Integer
, sprinkler_square_feet Integer
, roof_cover VARCHAR(20)
, bedrooms Integer
, bathrooms Decimal
, units Integer
, class_code VARCHAR(20)
, class_description VARCHAR(50)
, year_built Integer
, year_remodeled Integer
, adjusted_year_built Integer
, physical_age Integer
, built_as_length Integer
, built_as_width Integer
, mobile_home_model VARCHAR(50)
);
