
-- SOLAR EXPANSION
SELECT area,chipid,sum(area_km2) as summed_area FROM land_use_change 
WHERE
lulc_category_from != lulc_category_to
AND 
year_from = 2016 AND year_to = 2023
AND 
lulc_category_to in ('Solar Panel')
AND lulc_category_from not in ('Snow & Ice')

GROUP BY area,chipid
ORDER BY sum(area_km2) desc ;

-- RENEWABLE EXPANSION
SELECT area,chipid,sum(area_km2) as summed_area FROM land_use_change 
WHERE
lulc_category_from != lulc_category_to
AND 
year_from = 2016 AND year_to = 2023
AND 
lulc_category_to in ('Solar Panel','Wind Turbine')
AND lulc_category_from not in ('Snow & Ice')
AND chipid not in (SELECT chipid FROM land_use_change WHERE year_from = 2016 AND year_to = 2023 AND lulc_category_from = 'Wind Turbine')
GROUP BY area,chipid
ORDER BY sum(area_km2) desc;


--URBAN SPRAWL
SELECT area,chipid,sum(area_km2) as summed_area FROM land_use_change 
WHERE
lulc_category_from != lulc_category_to
AND 
year_from = 2016 AND year_to = 2023
AND 
lulc_category_to = 'Built Area'
AND lulc_category_from not in ('Snow & Ice')
GROUP BY area,chipid
ORDER BY sum(area_km2) desc;

--DESERTIFICATION
SELECT area,chipid,sum(area_km2) as summed_area FROM land_use_change 
WHERE
lulc_category_from != lulc_category_to
AND 
year_from = 2016 AND year_to = 2023
AND 
lulc_category_to = 'Bare ground'
AND lulc_category_from not in ('Snow & Ice')
GROUP BY area,chipid
ORDER BY sum(area_km2) desc;

--DEFORESTATION
SELECT area,chipid,sum(area_km2) as summed_area FROM land_use_change 
WHERE
lulc_category_from != lulc_category_to
AND 
year_from = 2016 AND year_to = 2023
AND 
lulc_category_from = 'Trees'
AND lulc_category_to not in ('Snow & Ice')
GROUP BY area,chipid
ORDER BY sum(area_km2) desc; 


--LARGE SCALE LULCC
SELECT country, Super_LULC_Category_From,Super_LULC_Category_To,sum(area_km2) as transferred_area
FROM (
SELECT 
area as country,
CASE WHEN 
lulc_category_from in ('Built Area','Crops')
THEN 'Artificial Land Use'
WHEN lulc_category_from in ('Wind Turbine','Solar Panel')
THEN 'Renewable Energy'
ELSE 'Natural Areas'
END
as Super_LULC_Category_From,

CASE WHEN 
lulc_category_to in ('Built Area','Crops')
THEN 'Artificial Land Use'
WHEN lulc_category_to in ('Wind Turbine','Solar Panel')
THEN 'Renewable Energy'
ELSE 'Natural Areas'
END
as Super_LULC_Category_To,

area_km2

FROM 

land_use_change
WHERE year_from = 2016 and year_to = 2023
and not ((lulc_category_from = 'Crops' AND lulc_category_to = 'Grass') OR (lulc_category_to = 'Crops' AND lulc_category_from = 'Grass'))
) iq
GROUP BY 
country, Super_LULC_Category_From,Super_LULC_Category_To
ORDER BY 
country,
sum(area_km2) desc;

--LAND USE COMPETITION

SELECT country,chipid, Super_LULC_Category_From,Super_LULC_Category_To,sum(area_km2) as transferred_area
FROM (
SELECT 
area as country,
chipid,
CASE WHEN 
lulc_category_from in ('Built Area','Crops')
THEN 'Artificial Land Use'
WHEN lulc_category_from in ('Wind Turbine','Solar Panel')
THEN 'Renewable Energy'
ELSE 'Natural Areas'
END
as Super_LULC_Category_From,

CASE WHEN 
lulc_category_to in ('Built Area','Crops')
THEN 'Artificial Land Use'
WHEN lulc_category_to in ('Wind Turbine','Solar Panel')
THEN 'Renewable Energy'
ELSE 'Natural Areas'
END
as Super_LULC_Category_To,

area_km2

FROM 

land_use_change
WHERE year_from = 2016 and year_to = 2023
AND lulc_category_from in ('Trees','Grass','Shrub & Scrub','Snow & Ice','Flooded vegetation','Bare ground')
AND lulc_category_to in ('Solar Panel','Wind Turbine')	
) iq
GROUP BY 
country,chipid ,Super_LULC_Category_From,Super_LULC_Category_To
ORDER BY 
sum(area_km2) desc;


--AGRICULTURAL EXPANSION
SELECT area,chipid,sum(area_km2) as summed_area FROM land_use_change 
WHERE
lulc_category_from != lulc_category_to
AND 
year_from = 2016 AND year_to = 2023
AND 
lulc_category_to = 'Crops'
AND lulc_category_from not in ('Snow & Ice','Grass')
GROUP BY area,chipid
ORDER BY sum(area_km2) desc
LIMIT 50;