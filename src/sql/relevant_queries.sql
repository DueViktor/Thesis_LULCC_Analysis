
CREATE INDEX landcover_index
ON lulv USING GIST (geometries);




SELECT area,year, CAST(SUM(area_in_square_kilometers) AS INT) as area_in_square_kilometers FROM(

select year, area , ST_Area(ST_Transform(geometries, 25832)) / 1000000.0 as area_in_square_kilometers from lulc 
where data_origins = 'DynamicWorld'
) iq
GROUP BY year, area

ORDER BY year desc


SELECT * FROM (
	SELECT area,year,chipid, CAST(SUM(area_in_square_kilometers) AS INT) as area_in_square_kilometers FROM(

select year, area, chipid , ST_Area(ST_Transform(geometries, 25832)) / 1000000.0 as area_in_square_kilometers from lulc 
where data_origins = 'DynamicWorld'
) iq
GROUP BY year, area, chipid
) iq 

ORDER BY area_in_square_kilometers desc,chipid,year;




select year,count(distinct chipid) num_chips ,count(*) num_polygons
from lulc 
group by year;





--DEDUPLICATION QUERY

WITH CTE AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY area, chipid, year,data_origins, MD5(ST_AsText(geometries))
            ORDER BY (SELECT NULL) -- Or specify a column to decide which row to keep
        ) AS rn
    FROM
        lulc
)
DELETE FROM lulc
WHERE ctid IN (
    SELECT ctid FROM CTE WHERE rn > 1
);

	
-- KILL ALL CONNECTIONS TO DATABASE

SELECT 
    pg_terminate_backend(pid)
FROM 
    pg_stat_activity
WHERE 
    -- Replace 'your_database_name' with the target database name
    datname = 'your_database_name'
    AND pid <> pg_backend_pid()
	AND usename =  DIT BRUGER NAVN HER; -- Exclude the current session



SELECT
    pid,
    datname AS database_name,
    usename AS user_name,
    state,
    query,
    query_start,
    application_name
FROM
    pg_stat_activity;




-- AREA VERIFICATION 
SELECT * FROM (
	SELECT area,year,polygon_id, CAST(SUM(area_in_square_kilometers) AS INT) as area_in_square_kilometers FROM(

select year, area, CAST(split_part(split_part(chipid, '_', 1), '-', 1) AS INT) as polygon_id, ST_Area(ST_Transform(geometries,25832))/1000000 as area_in_square_kilometers from lulc 
where data_origins = 'DynamicWorld'
) iq
GROUP BY year, area, polygon_id
) iq 

ORDER BY area,polygon_id,year;




SELECT * FROM (
SELECT area,year,chipid, SUM(area_in_square_kilometers)  as area_in_square_kilometers FROM(

select year, area, chipid , ST_Area(ST_Transform(geometries, 25832)) / 1000000.0 as area_in_square_kilometers from lulc 
where data_origins = 'SATLAS'
) iq
GROUP BY year, area, chipid
) oq
ORDER BY area_in_square_kilometers desc;



SELECT * FROM (
SELECT area,year, SUM(area_in_square_kilometers)  as area_in_square_kilometers FROM(

select year, area , ST_Area(ST_Transform(geometries, 25832)) / 1000000.0 as area_in_square_kilometers from lulc 
where data_origins = 'SATLAS'
) iq
GROUP BY year, area
) oq
ORDER BY area_in_square_kilometers desc;





-- Find the area used per name in a specific area for all years
SELECT area,name,year,ST_Area(ST_Transform(ST_Union(geometries), 25832)) / 1000000.0 AS result_geom_area_km2 from lulc WHERE area = "BLAH" AND WHERE data_origins = 'SATLAS' or data_origins = 'DynamicWorld' group by area,name,year;

SELECT ST_Area(ST_Transform(ST_Difference(DynamicWorld.lulc_polygon, SATLAS.lulc_polygon), 25832)) / 1000000.0 AS result_geom_area_km2

FROM (
SELECT 
	name, 
	ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2016-01-01'
    AND data_origins = 'DynamicWorld'
    AND chipid = '14_2_17'
	GROUP BY name
	) as DynamicWorld,

(
SELECT 
	--name, 
	ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2016-01-01'
    AND data_origins = 'SATLAS'
    AND chipid = '14_2_17'
--GROUP BY name
	) as SATLAS


UNION ALL

SELECT ST_Area(ST_Transform(ST_Difference(Solar.lulc_polygon, Wind.lulc_polygon), 25832)) / 1000000.0 AS result_geom_area_km2

FROM (
SELECT 
	name, 
	ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2016-01-01'
    AND data_origins = 'SATLAS'
    AND chipid = '14_2_17'
	AND name = 'Solar Panels'
	GROUP BY name
	) as Solar,

(
SELECT 
	--name, 
	ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2016-01-01'
    AND data_origins = 'SATLAS'
    AND chipid = '14_2_17'
	AND name = 'Wind Turbine'
--GROUP BY name
	) as Wind




UNION ALL

SELECT name, ST_Area(ST_Transform(ST_Difference(Solar.lulc_polygon, Wind.lulc_polygon), 25832)) / 1000000.0 AS result_geom_area_km2

FROM
(
SELECT 
	name, 
	ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2016-01-01'
    AND data_origins = 'SATLAS'
    AND chipid = '14_2_17'
	AND name = 'Wind Turbine'
 GROUP BY name
	) as Wind






SELECT *,intersection_area_sq_km/preceding_area_sq_km*100 as percent_change FROM (
SELECT
    preceding_year.name AS preceding_year_name,
    current_year.name AS current_year_name,
	 ST_Area(ST_Transform(preceding_year.lulc_polygon, 25832)) / 1000000.0 AS preceding_area_sq_km,
    ST_Area(ST_Transform(ST_Intersection(preceding_year.lulc_polygon, current_year.lulc_polygon), 25832)) / 1000000.0 AS intersection_area_sq_km
FROM (
    SELECT name, ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2016-01-01'
    AND data_origins = 'DynamicWorld'
    AND CAST(split_part(split_part(chipid, '_', 1), '-', 1) AS INT) = 1 -- POLYID
    GROUP BY name
) AS preceding_year
INNER JOIN (
    SELECT name, ST_Union(geometries) AS lulc_polygon
    FROM lulc
    WHERE area = 'Denmark'
    AND year = '2017-01-01'
    AND data_origins = 'DynamicWorld'
    AND CAST(split_part(split_part(chipid, '_', 1), '-', 1) AS INT) = 1 -- POLYID
    GROUP BY name
) AS current_year
ON ST_Intersects(preceding_year.lulc_polygon, current_year.lulc_polygon)
) ooq


ORDER BY 
intersection_area_sq_km DESC;






-- LAND USE AREA (VIKTOOOOOOR)

SELECT area,year_from,year_to,lulc_category_from,SUM(area_km2) as lulc_area
FROM land_use_change
GROUP BY area,year_from,year_to,lulc_category_from
ORDER BY 
area,year_from,year_to,SUM(area_km2) desc