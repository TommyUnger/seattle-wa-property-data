DROP TABLE IF EXISTS import.snohomish_sale_combined;

CREATE TABLE import.snohomish_sale_combined
AS
SELECT * FROM import.snohomish_sale_2016
UNION
SELECT * FROM import.snohomish_sale_2017
UNION
SELECT * FROM import.snohomish_sale_2018
UNION
SELECT * FROM import.snohomish_sale_2018_2
UNION
SELECT * FROM import.snohomish_sale_2019
;