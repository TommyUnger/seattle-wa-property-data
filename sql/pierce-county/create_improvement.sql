CREATE TABLE import.pierce_improvement(
parcel_number VARCHAR(10)
, building_id VARCHAR(5)
, property_type VARCHAR(15)
, neighborhood VARCHAR(10)
, neighborhood_extension VARCHAR(10)
, square_feet Integer
, net_square_feet Integer
, percent_complete Decimal
, condition VARCHAR(15)
, quality VARCHAR(15)
, primary_occupancy_code Integer
, primary_occupancy_description VARCHAR(255)
, mobile_home_serial_number VARCHAR(30)
, mobile_home_total_length Integer
, mobile_home_make VARCHAR(30)
, attic_finished_square_feet Integer
, basement_square_feet Integer
, basement_finished_square_feet Integer
, carport_square_feet Integer
, balcony_square_feet Integer
, porch_square_feet Integer
, attached_garage_square_feet Integer
, detached_garage_square_feet Integer
, fireplaces Integer
, basement_garage_door Integer
);
