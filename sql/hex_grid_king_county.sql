DROP TABLE IF EXISTS property.hex_grid;
CREATE TABLE property.hex_grid
AS
SELECT row_number()over() id
, geom
, ST_X(ST_Centroid(geom)) lon
, ST_Y(ST_Centroid(geom)) lat
FROM (
SELECT ST_transform(geom, 4269) geom
FROM
(select hexbin(1000, st_transform(st_setsrid(st_expand((SELECT ST_Extent(geom_centroid) FROM property.king_county_parcel_geo), 0.1), 4269), 900913)) geom) g
) t
;

CREATE INDEX king_county_hex_grid_idx ON property.hex_grid USING gist (geom);
CREATE INDEX king_county_hex_grid_idx_2 ON property.hex_grid(id);
