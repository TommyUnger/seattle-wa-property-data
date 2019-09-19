CREATE OR REPLACE FUNCTION hexbin (height numeric, minx numeric, miny numeric, maxx numeric, maxy numeric, srid integer)
RETURNS TABLE (geom geometry(polygon))
AS $$
    WITH d (width) AS (VALUES (height * 0.866)),
        hex (geom) AS (SELECT ST_GeomFromText(FORMAT('POLYGON((0 0, %s %s, %s %s, %s %s, %s %s, %s %s, 0 0))',
            width *  0.5, height * 0.25,
            width *  0.5, height * 0.75,
                       0, height,
            width * -0.5, height * 0.75,
            width * -0.5, height * 0.25
        ), srid) FROM d)
        SELECT
            ST_Translate(hex.geom, x_series, y_series)::geometry(polygon) geom
        FROM d, hex,
            generate_series(
                (minx / width)::int * width - width,
                (maxx / width)::int * width + width,
                width) x_series,
            generate_series(
                (miny / (height * 1.5))::int * (height * 1.5) - height,
                (maxy / (height * 1.5))::int * (height * 1.5) + height,
                height * 1.5) y_series
        UNION
        SELECT ST_Translate(hex.geom, x_series, y_series)::geometry(polygon) geom
        FROM d, hex,
            generate_series(
                (minx / width)::int * width - (width * 1.5),
                (maxx / width)::int * width + width,
                width) x_series,
            generate_series(
                (miny / (height * 1.5))::int * (height * 1.5) - (height * 1.75),
                (maxy / (height * 1.5))::int * (height * 1.5) + height,
                height * 1.5) y_series;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hexbin (height numeric, box box2d, srid integer)
RETURNS TABLE (geom geometry(polygon))
AS $$
    SELECT * FROM hexbin(height, st_xmin(box)::numeric, st_ymin(box)::numeric, st_xmax(box)::numeric, st_ymax(box)::numeric, srid);
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION hexbin (height numeric, box geometry)
RETURNS TABLE (geom geometry(polygon))
AS $$
    SELECT hex.geom
    FROM hexbin(height, st_xmin(box)::numeric, st_ymin(box)::numeric, st_xmax(box)::numeric, st_ymax(box)::numeric, st_srid(box)) hex(geom)
    WHERE _st_intersects(geom, box); -- skip the bounding box test in st_intersects because we know it's TRUE.
$$ LANGUAGE SQL IMMUTABLE;

