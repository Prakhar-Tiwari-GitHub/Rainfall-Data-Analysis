USE rainfall_database;

-- TOTAL MAXIMUM RAINFALL YEAR IN LAST 30 YEAR DIFFRENT STATE --

SELECT Subdivision, 
       Year,
       total_rainfall
FROM (
    SELECT 
        Subdivision,
        Year,
        ROUND(JAN + FEB + MAR + APR + MAY + JUN + JUL + AUG + SEP + OCT + NOV + DECS, 2) AS total_rainfall,
        RANK() OVER (PARTITION BY Subdivision ORDER BY (JAN + FEB + MAR + APR + MAY + JUN + JUL + AUG + SEP + OCT + NOV + DECS) DESC) AS rank_
    FROM rainfall_30yr_data 
) AS ranked_data
WHERE rank_ = 1;


-- MAXIMUN MONTHLY RAINFALL BY STATE IN LAST 30 YEAR --

SELECT *
FROM (
    SELECT 
        SUBDIVISION,
        Year,
        JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS,
        GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS Max_Monthly_Rainfall,
        RANK() OVER (PARTITION BY Subdivision ORDER BY (JAN + FEB + MAR + APR + MAY + JUN + JUL + AUG + SEP + OCT + NOV + DECS) DESC) AS rank_
    FROM rainfall_30yr_data 
) AS ranked_data
WHERE rank_ = 1;

-- MAXIMUN SUM OF RAINFALL BY STATE -- 

SELECT 
    Subdivision,
    ROUND(SUM(JAN + FEB + MAR + APR + MAY + JUN + JUL + AUG + SEP + OCT + NOV + DECS)) AS total_rainfall
FROM rainfall_30yr_data AS R
GROUP BY Subdivision
ORDER BY total_rainfall DESC
LIMIT 1;

-- MAXIMUM MONTHLY RAINFALL IN ALL STATE --

WITH MaxRainfallMonth AS (
    SELECT 
        Subdivision,
        Year,
        -- Using GREATEST to get the month with the highest rainfall
        GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS max_rainfall,
        -- Identifying the month corresponding to the highest rainfall
        CASE
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JAN THEN 'JAN'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = FEB THEN 'FEB'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAR THEN 'MAR'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = APR THEN 'APR'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAY THEN 'MAY'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUN THEN 'JUN'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUL THEN 'JUL'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = AUG THEN 'AUG'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = SEP THEN 'SEP'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = OCT THEN 'OCT'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = NOV THEN 'NOV'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = DECS THEN 'DEC'
        END AS max_rainfall_month
    FROM rainfall_30yr_data 
)

SELECT
	Subdivision, 
	Year, 
	max_rainfall_month, 
	max_rainfall 
FROM(
	SELECT 
		Subdivision, 
		Year, 
		max_rainfall_month, 
		max_rainfall,
		RANK() OVER(PARTITION BY Subdivision ORDER BY max_rainfall DESC) AS RANKING
	FROM MaxRainfallMonth ) AS A
WHERE RANKING = 1
ORDER BY max_rainfall DESC
LIMIT 1; 

-- CREATING WINDOW FUNTION TO GET THE MAXIMUM RAINFALL MONTH NAME IN EVERY YEAR OF EACH STATE --

WITH MaxRainfallMonth AS (
    SELECT 
        Subdivision,
        Year,
        -- Using GREATEST to get the month with the highest rainfall
        GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS max_rainfall,
        -- Identifying the month corresponding to the highest rainfall
        CASE
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JAN THEN 'JAN'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = FEB THEN 'FEB'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAR THEN 'MAR'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = APR THEN 'APR'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAY THEN 'MAY'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUN THEN 'JUN'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUL THEN 'JUL'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = AUG THEN 'AUG'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = SEP THEN 'SEP'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = OCT THEN 'OCT'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = NOV THEN 'NOV'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = DECS THEN 'DEC'
        END AS max_rainfall_month
    FROM rainfall_30yr_data 
)

SELECT 
	Subdivision, 
	Year, 
	max_rainfall_month, 
	max_rainfall,
	RANK() OVER(PARTITION BY Subdivision ORDER BY max_rainfall DESC) AS RANKING
FROM MaxRainfallMonth  AS A;

-- CREATING WINDOW FUNTION TO RETRIVE MONTH NAME WITH MAX RAINFALL IN 30 YEAR FOR STATE  --

WITH MaxRainfallMonth AS (
    SELECT 
        Subdivision,
        Year,
        -- Using GREATEST to get the month with the highest rainfall
        GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS max_rainfall,
        -- Identifying the month corresponding to the highest rainfall
        CASE
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JAN THEN 'JAN'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = FEB THEN 'FEB'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAR THEN 'MAR'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = APR THEN 'APR'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAY THEN 'MAY'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUN THEN 'JUN'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUL THEN 'JUL'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = AUG THEN 'AUG'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = SEP THEN 'SEP'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = OCT THEN 'OCT'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = NOV THEN 'NOV'
            WHEN GREATEST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = DECS THEN 'DEC'
        END AS max_rainfall_month
    FROM rainfall_30yr_data 
)


SELECT
	* 
FROM(
	SELECT 
		Subdivision, 
		Year, 
		max_rainfall_month, 
		max_rainfall,
		RANK() OVER(PARTITION BY Subdivision ORDER BY max_rainfall DESC) AS RANKING
	FROM MaxRainfallMonth ) AS A
WHERE RANKING = 1
ORDER BY Subdivision ; 

-- MINIMUM RAINFALL IN EVERY STATE IN EVRY YEAR OF LAST 30 YEAR --

SELECT
	SUBDIVISION,
	Year,
	min_rainfall
FROM
	(SELECT 
		SUBDIVISION,
		Year,
		LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS min_rainfall,
		ROW_NUMBER() OVER(PARTITION BY SUBDIVISION ) AS Min_Rainfall_Ranking
	FROM 
	rainfall_30yr_data AS R) AS A
ORDER BY min_rainfall ASC
LIMIT 1;

-- MINIMUN RAINFALL IN EVERY STATE IN 30 YEAR --

   -- creating window funtion to get the name of the month --
WITH MinRainfallMonth AS (
    SELECT 
        Subdivision,
        Year,
        LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS min_rainfall,
        CASE
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JAN THEN 'JAN'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = FEB THEN 'FEB'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAR THEN 'MAR'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = APR THEN 'APR'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAY THEN 'MAY'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUN THEN 'JUN'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUL THEN 'JUL'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = AUG THEN 'AUG'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = SEP THEN 'SEP'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = OCT THEN 'OCT'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = NOV THEN 'NOV'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = DECS THEN 'DEC'
        END AS min_rainfall_month
    FROM rainfall_30yr_data 
)


SELECT
	Subdivision, 
	Year, 
	min_rainfall_month, 
	min_rainfall
FROM(
	SELECT 
		Subdivision, 
		Year, 
		min_rainfall_month, 
		min_rainfall,
		RANK() OVER(PARTITION BY Subdivision ORDER BY min_rainfall ASC) AS RANKING
	FROM MinRainfallMonth ) AS A
WHERE RANKING = 1
ORDER BY Subdivision; 

-- STATE WHERE NO MONTH OR YEAR WITH 0 RAINFALL --

WITH MinRainfallMonth AS (
    SELECT 
        Subdivision,
        Year,
        LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS min_rainfall,
        CASE
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JAN THEN 'JAN'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = FEB THEN 'FEB'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAR THEN 'MAR'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = APR THEN 'APR'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAY THEN 'MAY'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUN THEN 'JUN'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUL THEN 'JUL'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = AUG THEN 'AUG'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = SEP THEN 'SEP'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = OCT THEN 'OCT'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = NOV THEN 'NOV'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = DECS THEN 'DEC'
        END AS min_rainfall_month
    FROM rainfall_30yr_data 
)


SELECT
    Subdivision, 
    Year, 
    min_rainfall_month, 
    min_rainfall
FROM (
    SELECT 
        Subdivision, 
        Year, 
        min_rainfall_month, 
        min_rainfall,
        RANK() OVER(PARTITION BY Subdivision ORDER BY min_rainfall ASC) AS RANKING
    FROM MinRainfallMonth
) AS A
WHERE min_rainfall != 0 AND RANKING = 1
ORDER BY Subdivision;

-- STATE WITH MINIMUM RAINFALL BUT NOT EQUAL TO 0 IN LAST 30YEAR --


WITH MinRainfallMonth AS (
    SELECT 
        Subdivision,
        Year,
        LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) AS min_rainfall,
        CASE
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JAN THEN 'JAN'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = FEB THEN 'FEB'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAR THEN 'MAR'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = APR THEN 'APR'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = MAY THEN 'MAY'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUN THEN 'JUN'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = JUL THEN 'JUL'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = AUG THEN 'AUG'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = SEP THEN 'SEP'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = OCT THEN 'OCT'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = NOV THEN 'NOV'
            WHEN LEAST(JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DECS) = DECS THEN 'DEC'
        END AS min_rainfall_month
    FROM rainfall_30yr_data
)

SELECT
    Subdivision,
    MIN(min_rainfall) AS Min_Rainfall
FROM MinRainfallMonth
GROUP BY Subdivision 
HAVING SUM(CASE WHEN min_rainfall = 0 THEN 1 ELSE 0 END) = 0
ORDER BY Subdivision;   


-- STATE WITH MOST DRY SEASON IN LAST 30 YEAR --

SELECT
	SUBDIVISION,
	SUM(Total_Dry_Months) AS Sum_Of_Total_Dry_Month
	FROM(
	SELECT 
		SUBDIVISION,
		Year,
		(CASE 
			WHEN JAN = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN FEB = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN MAR = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN APR = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN MAY = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN JUN = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN JUL = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN AUG = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN SEP = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN OCT = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN NOV = 0 THEN 1 ELSE 0 END +
		CASE 
			WHEN DECS = 0 THEN 1 ELSE 0 END
		)AS Total_Dry_Months
	FROM 
		rainfall_30yr_data ) B
GROUP BY SUBDIVISION
ORDER BY Sum_Of_Total_Dry_Month DESC;

-- STATE WITH MOSTLY RAINY SEASON --

WITH RainyMonths AS (
    SELECT 
        SUBDIVISION,
        SUM(CASE 
            WHEN JAN = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN FEB = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN MAR = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN APR = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN MAY = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN JUN = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN JUL = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN AUG = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN SEP = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN OCT = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN NOV = 0 THEN 0 ELSE 1 END +
            CASE 
            WHEN DECS = 0 THEN 0 ELSE 1 END
        ) AS Total_Rainy_Months
    FROM 
        rainfall_30yr_data
    GROUP BY 
        SUBDIVISION
)
SELECT 
    SUBDIVISION,
    Total_Rainy_Months AS Mostly_Rainy_State
FROM 
    RainyMonths
WHERE 
    Total_Rainy_Months = (SELECT MAX(Total_Rainy_Months) FROM RainyMonths)
ORDER BY 
    Mostly_Rainy_State DESC;
    
-- FINDING MAX ANUAL RAINFALL--

SELECT 
	SUBDIVISION,
    Year,
	MAX(ANNUAL) Maximum_Annual_Rainfall
FROM 
rainfall_anually
GROUP BY SUBDIVISION, Year
ORDER BY Maximum_Annual_Rainfall DESC;

-- FINDING MIN ANNUAL RAINFALL --

SELECT 
	SUBDIVISION,
    YEAR, 
    MIN(ANNUAL) AS Minimum_Annual_Rainfall
FROM rainfall_anually
GROUP BY SUBDIVISION, YEAR
ORDER BY Minimum_Annual_Rainfall ASC;

-- ANNUAL RAINFALL GREATER THAN AVRAGE RAINFALL --

SELECT 
	SUBDIVISION,
    COUNT(ANNUAL) Annual_Rainfall_More_Than_Average
FROM rainfall_anually
WHERE ANNUAL >(	SELECT 
					ROUND(AVG(ANNUAL), 2) AS Average_Rainfall
				FROM rainfall_anually )
GROUP BY SUBDIVISION;

-- MAXIMUM MONTHLY RANGE RAIN --

SELECT 
	SUBDIVISION,
    YEAR,
    GREATEST(JF, MAM, JJAS, OND) AS Maximum_Monthly_Rain_Range,
    CASE 
		WHEN GREATEST(JF, MAM, JJAS, OND) = JF THEN 'JF'
		WHEN GREATEST(JF, MAM, JJAS, OND) = MAM THEN 'MAM'
		WHEN GREATEST(JF, MAM, JJAS, OND) = JJAS THEN 'JJAS'
        WHEN GREATEST(JF, MAM, JJAS, OND) = OND THEN 'OND'
	END AS Maximum_Rain_Month_Range
FROM rainfall_monthly_range;

-- ALL TIME MAXIMUM RAINFALL --

SELECT
	SUBDIVISION,
    YEAR,
    MAX(ANNUAL) AS Maximum_Annual_Rainfall_Of_All_Time
FROM 
	rainfall_anually
GROUP BY 
	SUBDIVISION,
    YEAR
ORDER BY
	Maximum_Annual_Rainfall_Of_All_Time DESC
LIMIT 1;
