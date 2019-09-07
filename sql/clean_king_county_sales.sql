select excise_tax_nbr::INT excise_tax_nbr
, major::INT major
, minor::SMALLINT minor
, major::BIGINT*10000 + minor::BIGINT pin
, document_date::DATE sale_date
, sale_price::INT sale_price
, recording_nbr::BIGINT recording_nbr
, seller_name::VARCHAR(150) seller_name
, buyer_name::VARCHAR(150) buyer_name
, lu1.descr::king_county_type_1 property_type
, lu2.descr::king_county_type_2 principal_use
, lu6.descr::king_county_type_6 sale_instrument
, CASE WHEN afforest_land in ('Y', '1') THEN 1 ELSE 0 END::BOOL af_forest_land
, CASE WHEN afcurrent_use_land in ('Y', '1') THEN 1 ELSE 0 END::BOOL af_current_use_land
, CASE WHEN afnon_profit_use in ('Y', '1') THEN 1 ELSE 0 END::BOOL af_non_profit_use
, CASE WHEN afhistoric_property in ('Y', '1') THEN 1 ELSE 0 END::BOOL af_historic_property
, lu5.descr::king_county_type_5 sale_reason
, lu4.descr::king_county_type_4 property_class
, string_to_array(regexp_replace(sale_warning, '[^0-9 ]+', '0', 'g'), ' ')::integer[] sale_warnings
FROM import.king_county_sales d
LEFT JOIN property.king_county_lookup lu1 ON lu1.lu_type = 1 AND lu1.lu_item = d.property_type::SMALLINT
LEFT JOIN property.king_county_lookup lu2 ON lu2.lu_type = 2 AND lu1.lu_item = d.principal_use::SMALLINT
LEFT JOIN property.king_county_lookup lu6 ON lu6.lu_type = 6 AND lu6.lu_item = d.sale_instrument::SMALLINT
LEFT JOIN property.king_county_lookup lu5 ON lu5.lu_type = 5 AND lu5.lu_item = d.sale_reason::SMALLINT
LEFT JOIN property.king_county_lookup lu4 ON lu4.lu_type = 4 AND lu4.lu_item = d.property_class::SMALLINT