-- B1: Tạo bảng trữ dữ liệu RAW
SELECT *
FROM worldlifeexpectancy_staging;

-- B2: Đổi datatypes cột life expectancy về decimal (4,2)
SET SQL_SAFE_UPDATES = 0;

UPDATE worldlifexpectancy
SET Lifeexpectancy = NULL
WHERE Lifeexpectancy ='';

ALTER TABLE worldlifexpectancy
MODIFY COLUMN Lifeexpectancy DECIMAL(4,2);

-- Đổi tên cột để chạy syntax không lỗi
ALTER TABLE worldlifexpectancy
RENAME COLUMN `under-fivedeaths` TO `under_fivedeaths`;

# column name thinness1-19years should be change to 10-19 to correctly indicate the data
  
ALTER TABLE worldlifexpectancy
RENAME COLUMN `thinness1-19years` TO `thinness10_19years`
, RENAME COLUMN `thinness5-9years` TO `thinness5_9years`;

/* 1. Handling missing values
- Check column with blank/null value
- Turn all blank value to null for easy handling
*/

# Check cột có blank/null value
SELECT
COUNT(*) - COUNT(CASE WHEN Country = '' THEN NULL ELSE Country END) AS Country
, COUNT(*) - COUNT(CASE WHEN Year = '' THEN NULL ELSE Year END) AS Year
, COUNT(*) - COUNT(CASE WHEN Status = '' THEN NULL ELSE Status END) AS Status
, COUNT(*) - COUNT(CASE WHEN Lifeexpectancy = '' THEN NULL ELSE Lifeexpectancy END) AS Lifeexpectancy
, COUNT(*) - COUNT(CASE WHEN AdultMortality = '' THEN NULL ELSE AdultMortality END) AS AdultMortality
, COUNT(*) - COUNT(CASE WHEN infantdeaths = '' THEN NULL ELSE infantdeaths END) AS infantdeaths
, COUNT(*) - COUNT(CASE WHEN percentageexpenditure = '' THEN NULL ELSE percentageexpenditure END) AS percentageexpenditure
, COUNT(*) - COUNT(CASE WHEN Measles = '' THEN NULL ELSE Measles END) AS Measles
, COUNT(*) - COUNT(CASE WHEN BMI = '' THEN NULL ELSE BMI END) AS BMI
, COUNT(*) - COUNT(CASE WHEN under_fivedeaths = '' THEN NULL ELSE under_fivedeaths END) AS under_fivedeaths
, COUNT(*) - COUNT(CASE WHEN Polio = '' THEN NULL ELSE Polio END) AS Polio
, COUNT(*) - COUNT(CASE WHEN Diphtheria = '' THEN NULL ELSE Diphtheria END) AS Diphtheria
, COUNT(*) - COUNT(CASE WHEN HIVAIDS = '' THEN NULL ELSE HIVAIDS END) AS HIVAIDS
, COUNT(*) - COUNT(CASE WHEN GDP = '' THEN NULL ELSE GDP END) AS GDP
, COUNT(*) - COUNT(CASE WHEN thinness10_19years = '' THEN NULL ELSE thinness10_19years END) AS thinness10_19years
, COUNT(*) - COUNT(CASE WHEN thinness5_9years = '' THEN NULL ELSE thinness5_9years END) AS thinness5_9years
, COUNT(*) - COUNT(CASE WHEN Schooling = '' THEN NULL ELSE Schooling END) AS Schooling
, COUNT(*) - COUNT(CASE WHEN Row_ID = '' THEN NULL ELSE Row_ID END) AS Row_ID
FROM worldlifexpectancy;


-- Đổi blank -> null
SET SQL_SAFE_UPDATES = 0;

UPDATE worldlifexpectancy
SET Status = NULL
WHERE Status ='';

UPDATE worldlifexpectancy
SET AdultMortality = NULL
WHERE AdultMortality ='';

UPDATE worldlifexpectancy
SET infantdeaths = NULL
WHERE infantdeaths ='';

UPDATE worldlifexpectancy
SET percentageexpenditure = NULL
WHERE percentageexpenditure  ='';

UPDATE worldlifexpectancy
SET Measles = NULL
WHERE Measles  ='';

UPDATE worldlifexpectancy
SET BMI = NULL
WHERE BMI  ='';

UPDATE worldlifexpectancy
SET under_fivedeaths = NULL
WHERE under_fivedeaths ='';

UPDATE worldlifexpectancy
SET Polio = NULL
WHERE Polio  ='';

UPDATE worldlifexpectancy
SET Diphtheria = NULL
WHERE Diphtheria  ='';

UPDATE worldlifexpectancy
SET GDP = NULL
WHERE GDP  ='';

UPDATE worldlifexpectancy
SET thinness1_19years = NULL
WHERE thinness1_19years  ='';

UPDATE worldlifexpectancy
SET thinness5_9years = NULL
WHERE thinness5_9years  ='';

UPDATE worldlifexpectancy
SET Schooling = NULL
WHERE  Schooling ='';

SET SQL_SAFE_UPDATES = 1;

/* 1. Handling missing values
- Columns with null values: 
	+ Categorical: Status => set null equal to nearest last year
    + numerical:
		o Lifeexpectancy: linear interpolation set = AVG(last year+next year); if last-observed values then take nearest one)
        o AdultMortality: linear interpolation for missing data
        o infantdeath: fill by mean of the country or if fully null, fill by mean of developed/developing country in that year
        o under_fivedeaths: similar to infantdeath
        o BMI: fill by mean of country/mean of year
        o Measeles, Polio, Diphtheria: median
        o GDP: tendency to increase -> linear interpolation, if all missing => avg year+status
        o thinness10_19years;thinness5_9years: fill = mean
        o Schooling: = linear interpolation
*/

SELECT Country
, COUNT(*) - COUNT(GDP) AS null_value
FROM worldlifexpectancy
GROUP BY Country
ORDER BY null_value DESC, Country;


# Fill missing data for Status column (LAG 1 year)
WITH lag_cte AS
(
SELECT Row_ID
, LAG(Status) OVER (PARTITION BY Country ORDER BY Year ASC)  AS to_fill
FROM worldlifexpectancy
)

UPDATE worldlifexpectancy
INNER JOIN lag_cte
ON worldlifexpectancy.Row_ID = lag_cte.Row_ID
	AND worldlifexpectancy.Status IS NULL
SET worldlifexpectancy.Status = lag_cte.to_fill;

/* LIFE EXPECTANCY
Linear interpolation if missing value has value of last year and next year
If last/first observed value, copy the nearest value
IF no value detected take avg by status and year
*/
WITH update_life_expectancy AS
(
SELECT Row_ID 
, Country
, Status
, Year
, Lifeexpectancy
, CASE 
	WHEN Lifeexpectancy IS NOT NULL THEN Lifeexpectancy
    WHEN LAG(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
		AND LEAD(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
        THEN (LAG(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year)+LEAD(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year))/2
	WHEN LAG(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		AND LEAD(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
        THEN AVG(Lifeexpectancy) OVER (PARTITION BY Status,Year)
	WHEN LAG(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		THEN FIRST_VALUE(Lifeexpectancy) OVER (PARTITION BY Country ORDER BY Year)
	WHEN LEAD(Lifeexpectancy,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		THEN FIRST_VALUE(Lifeexpectancy) OVER (PARTITION BY Country ORDER BY Year DESC)
	END AS new_Lifeexpectancy
FROM worldlifexpectancy
ORDER BY Country, Year 
)

UPDATE worldlifexpectancy
INNER JOIN update_life_expectancy
ON worldlifexpectancy.Row_ID = update_life_expectancy.Row_ID
	AND worldlifexpectancy.Lifeexpectancy IS NULL
SET worldlifexpectancy.Lifeexpectancy = update_life_expectancy.new_Lifeexpectancy;

/* ADULT MORTALITY
Linear interpolation if missing value has value of last year and next year
If last/first observed value, copy the nearest value
IF no value detected take avg by status and year
*/

WITH update_adult_mortality AS
(
SELECT Row_ID 
, Country
, Status
, Year
, AdultMortality
, CASE 
	WHEN AdultMortality IS NOT NULL THEN AdultMortality
    WHEN LAG(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
		AND LEAD(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
        THEN (LAG(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year)+LEAD(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year))/2
	WHEN LAG(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		AND LEAD(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
        THEN AVG(AdultMortality) OVER (PARTITION BY Status,Year)
	WHEN LAG(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		THEN FIRST_VALUE(AdultMortality) OVER (PARTITION BY Country ORDER BY Year)
	WHEN LEAD(AdultMortality,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		THEN FIRST_VALUE(AdultMortality) OVER (PARTITION BY Country ORDER BY Year DESC)
	END AS new_AdultMortality
FROM worldlifexpectancy
ORDER BY Country, Year 
)

UPDATE worldlifexpectancy
INNER JOIN update_adult_mortality
ON worldlifexpectancy.Row_ID = update_adult_mortality.Row_ID
	AND worldlifexpectancy.AdultMortality IS NULL
SET worldlifexpectancy.AdultMortality = update_adult_mortality.new_AdultMortality;

/* SCHOOLING
Linear interpolation if missing value has value of last year and next year
If last/first observed value, copy the nearest value
IF no value detected take avg by status and year
*/

WITH update_schooling AS
(
SELECT Row_ID 
, Country
, Status
, Year
, Schooling
, CASE 
	WHEN Schooling IS NOT NULL THEN Schooling
    WHEN LAG(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
		AND LEAD(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
        THEN ROUND((LAG(Schooling,1) OVER (PARTITION BY Country ORDER BY Year)+LEAD(Schooling,1) OVER (PARTITION BY Country ORDER BY Year))/2,2)
	WHEN LAG(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		AND LEAD(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
        THEN ROUND(AVG(Schooling) OVER (PARTITION BY Status,Year),2)
	WHEN LAG(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
    AND LEAD(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL
		THEN LEAD(Schooling,1) OVER (PARTITION BY Country ORDER BY Year)
	WHEN LEAD(Schooling,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		THEN FIRST_VALUE(Schooling) OVER (PARTITION BY Country ORDER BY Year DESC)
	END AS new_Schooling
FROM worldlifexpectancy
ORDER BY Country, Year 
)

UPDATE worldlifexpectancy
INNER JOIN update_schooling
ON worldlifexpectancy.Row_ID = update_schooling.Row_ID
SET worldlifexpectancy.Schooling = update_schooling.new_Schooling;

/* GDP
Linear interpolation if missing value has value of last year and next year
If last/first observed value, copy the nearest value
IF no value detected take avg by status and year
*/

WITH update_GDP AS
(
SELECT Row_ID 
, Country
, Status
, Year
, GDP
, CASE 
	WHEN GDP IS NOT NULL THEN GDP
    WHEN LAG(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
		AND LEAD(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL 
        THEN ROUND((LAG(GDP,1) OVER (PARTITION BY Country ORDER BY Year)+LEAD(GDP,1) OVER (PARTITION BY Country ORDER BY Year))/2,2)
	WHEN LAG(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
		AND LEAD(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
        THEN ROUND(AVG(GDP) OVER (PARTITION BY Status,Year),2)
	WHEN LAG(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
    AND LEAD(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL
		THEN LEAD(GDP,1) OVER (PARTITION BY Country ORDER BY Year)
	WHEN LEAD(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NULL 
    AND LAG(GDP,1) OVER (PARTITION BY Country ORDER BY Year) IS NOT NULL
		THEN LAG(GDP,1) OVER (PARTITION BY Country ORDER BY Year) 
	END AS new_GDP
FROM worldlifexpectancy
ORDER BY Country, Year 
)

UPDATE worldlifexpectancy
INNER JOIN update_GDP
ON worldlifexpectancy.Row_ID = update_GDP.Row_ID
SET worldlifexpectancy.GDP = update_GDP.new_GDP;

/* infantdeaths, under_fivedeaths, thinness10_19years, thinness5_9years Polio, Diphtheria, BMI, Measles, percentageexpenditure
mean
IF no value detected take mean by status and year
*/


WITH update_diseases AS
(
SELECT Row_ID 
, Country
, Status
, Year
, CASE 
	WHEN infantdeaths IS NOT NULL THEN infantdeaths
    WHEN COUNT(infantdeaths) OVER (PARTITION BY Country) >0 THEN AVG(infantdeaths) OVER (PARTITION BY Country)
    ELSE AVG(infantdeaths) OVER (PARTITION BY Status, Year)
END AS new_infantdeaths
, CASE 
	WHEN under_fivedeaths IS NOT NULL THEN under_fivedeaths
    WHEN COUNT(under_fivedeaths) OVER (PARTITION BY Country) >0 THEN AVG(under_fivedeaths) OVER (PARTITION BY Country)
    ELSE AVG(under_fivedeaths) OVER (PARTITION BY Status, Year)
END AS new_under_fivedeaths
, CASE 
	WHEN thinness10_19years IS NOT NULL THEN thinness10_19years
    WHEN COUNT(thinness10_19years) OVER (PARTITION BY Country) >0 THEN AVG(thinness10_19years) OVER (PARTITION BY Country)
    ELSE AVG(thinness10_19years) OVER (PARTITION BY Status, Year)
END AS new_thinness10_19years
, CASE 
	WHEN thinness5_9years IS NOT NULL THEN thinness5_9years
    WHEN COUNT(thinness5_9years) OVER (PARTITION BY Country) >0 THEN AVG(thinness5_9years) OVER (PARTITION BY Country)
    ELSE AVG(thinness5_9years) OVER (PARTITION BY Status, Year)
END AS new_thinness5_9years
, CASE 
	WHEN Polio IS NOT NULL THEN Polio
    WHEN COUNT(Polio) OVER (PARTITION BY Country) >0 THEN AVG(Polio) OVER (PARTITION BY Country)
    ELSE AVG(Polio) OVER (PARTITION BY Status, Year)
END AS new_Polio
, CASE 
	WHEN Diphtheria IS NOT NULL THEN Diphtheria
    WHEN COUNT(Diphtheria) OVER (PARTITION BY Country) >0 THEN AVG(Diphtheria) OVER (PARTITION BY Country)
    ELSE AVG(Diphtheria) OVER (PARTITION BY Status, Year)
END AS new_Diphtheria
, CASE 
	WHEN BMI IS NOT NULL THEN BMI
    WHEN COUNT(BMI) OVER (PARTITION BY Country) >0 THEN AVG(BMI) OVER (PARTITION BY Country)
    ELSE AVG(BMI) OVER (PARTITION BY Status, Year)
END AS new_BMI
, CASE 
	WHEN Measles IS NOT NULL THEN Measles
    WHEN COUNT(Measles) OVER (PARTITION BY Country) >0 THEN AVG(Measles) OVER (PARTITION BY Country)
    ELSE AVG(Measles) OVER (PARTITION BY Status, Year)
END AS new_Measles
, CASE 
	WHEN percentageexpenditure IS NOT NULL THEN percentageexpenditure
    WHEN COUNT(percentageexpenditure) OVER (PARTITION BY Country) >0 THEN AVG(percentageexpenditure) OVER (PARTITION BY Country)
    ELSE AVG(percentageexpenditure) OVER (PARTITION BY Status, Year)
END AS new_percentageexpenditure
FROM worldlifexpectancy
ORDER BY Country, Year 
)

UPDATE worldlifexpectancy
INNER JOIN update_diseases
ON worldlifexpectancy.Row_ID = update_diseases.Row_ID
SET worldlifexpectancy.infantdeaths = update_diseases.new_infantdeaths
, worldlifexpectancy.under_fivedeaths = update_diseases.new_under_fivedeaths
, worldlifexpectancy.thinness10_19years = update_diseases.new_thinness10_19years
, worldlifexpectancy.thinness5_9years = update_diseases.new_thinness5_9years
, worldlifexpectancy.Polio = update_diseases.new_Polio
, worldlifexpectancy.Diphtheria = update_diseases.new_Diphtheria
, worldlifexpectancy.BMI = update_diseases.new_BMI
, worldlifexpectancy.Measles = update_diseases.new_Measles
, worldlifexpectancy.percentageexpenditure = update_diseases.new_percentageexpenditure;

# 2. Data Consistency: Standardize categorical data -> after checking this is not needed

# 3. Removing duplicates:
WITH check_dup AS
(
SELECT Row_ID
, Country
, Year
, ROW_NUMBER() OVER (PARTITION BY Country, Year ORDER BY Year) AS dup_index
FROM worldlifexpectancy
)

DELETE FROM worldlifexpectancy
WHERE Row_ID IN (SELECT Row_ID
FROM check_dup
WHERE dup_index>1);

# 4. Outlier Detection and Treatment
/* 
infantdeaths, Lifeexpectancy, under_fivedeaths, schooling do not have extreme unreasonable outliers so I will leave it for now
percentageexpenditure has unreasonable number for being a ratio of heathcare expenditure over GDP (average world health expenditure is around 10%), consider this is not percentage but amount of expenditure
=> divide by GDP to get actual percentage
*/

ALTER TABLE worldlifexpectancy
RENAME COLUMN `percentageexpenditure` TO `health_expenditure`
, ADD COLUMN `percentageexpenditure` DECIMAL(4,2);

/* AdultMortality has strange pattern of dropping from 3 or 2 digit number to 1 digit number, 
it seems like a inputting errors => drop these value and replace by avg
*/
WITH mortality_clean AS
(
SELECT Row_ID
, Country
, Year
, AdultMortality
, AVG(AdultMortality) OVER (partition by Country)/3 AS threshold
, ROUND(AVG(AdultMortality) OVER (partition by Country),2) AS avg_mortality
FROM worldlifexpectancy
)

UPDATE worldlifexpectancy WLE
JOIN mortality_clean MC
ON WLE.Row_ID = MC.Row_ID
	AND MC.AdultMortality < MC.threshold
SET WLE.AdultMortality = MC.avg_mortality;

/* update similarly for health_expenditure, Polio, Diphtheria, GDP, thinness10_19years, thinness5_9years
*/

CREATE TEMPORARY TABLE disease_clean AS
(
SELECT Row_ID
, Country
, Year
, Polio
, AVG(Polio) OVER (partition by Country)/3 AS threshold_Polio
, ROUND(AVG(Polio) OVER (partition by Country),0) AS avg_Polio
, Diphtheria
, AVG(Diphtheria) OVER (partition by Country)/3 AS threshold_Diphtheria
, ROUND(AVG(Diphtheria) OVER (partition by Country),0) AS avg_Diphtheria
, thinness10_19years
, AVG(thinness10_19years) OVER (partition by Country)/3 AS threshold_thinness10_19years
, ROUND(AVG(thinness10_19years) OVER (partition by Country),2) AS avg_thinness10_19years
, thinness5_9years
, AVG(thinness5_9years) OVER (partition by Country)/3 AS threshold_thinness5_9years
, ROUND(AVG(thinness5_9years) OVER (partition by Country),2) AS avg_thinness5_9years
, GDP
, AVG(GDP) OVER (partition by Country)/3 AS threshold_GDP
, ROUND(AVG(GDP) OVER (partition by Country),2) AS avg_GDP
, health_expenditure
, AVG(health_expenditure) OVER (partition by Country)/3 AS threshold_health_expenditure
, ROUND(AVG(health_expenditure) OVER (partition by Country),2) AS avg_health_expenditure
FROM worldlifexpectancy
);

UPDATE worldlifexpectancy WLE
JOIN disease_clean DC
ON WLE.Row_ID = DC.Row_ID
SET WLE.Polio = DC.avg_Polio WHERE WLE.Polio < DC.threshold_Polio;

UPDATE worldlifexpectancy WLE
JOIN disease_clean DC
ON WLE.Row_ID = DC.Row_ID
SET WLE.Diphtheria = DC.avg_Diphtheria WHERE WLE.Diphtheria < DC.threshold_Diphtheria;

UPDATE worldlifexpectancy WLE
JOIN disease_clean DC
ON WLE.Row_ID = DC.Row_ID
SET WLE.GDP = DC.avg_GDP WHERE WLE.GDP < DC.threshold_GDP;

UPDATE worldlifexpectancy WLE
JOIN disease_clean DC
ON WLE.Row_ID = DC.Row_ID
SET WLE.thinness10_19years = DC.avg_thinness10_19years WHERE WLE.thinness10_19years < DC.threshold_thinness10_19years;

UPDATE worldlifexpectancy WLE
JOIN disease_clean DC
ON WLE.Row_ID = DC.Row_ID
SET WLE.thinness5_9years = DC.avg_thinness5_9years WHERE WLE.thinness5_9years < DC.threshold_thinness5_9years;

UPDATE worldlifexpectancy WLE
JOIN disease_clean DC
ON WLE.Row_ID = DC.Row_ID
SET WLE.health_expenditure = DC.avg_health_expenditure WHERE WLE.health_expenditure < DC.threshold_health_expenditure;

# BMI: drop if out of range 15-40; replace by average 
WITH BMI_outliers AS
(
	SELECT Row_ID
    , Country
	, Year
	, BMI
	FROM worldlifexpectancy
	WHERE BMI NOT BETWEEN 15 AND 40
)
, avg_bmi_normal AS
(
	SELECT Country
	, AVG(BMI) AS avg_bmi
	FROM worldlifexpectancy
	WHERE BMI BETWEEN 15 AND 40
	GROUP BY Country
)
, BMI_clean AS
(
	SELECT BMI_outliers.Row_ID
    , BMI_outliers.Country
	, BMI_outliers.Year
	, BMI_outliers.BMI
    , avg_bmi_normal.avg_bmi
	FROM BMI_outliers
    JOIN avg_bmi_normal
    ON BMI_outliers.Country = avg_bmi_normal.Country
)

UPDATE worldlifexpectancy WLE
JOIN BMI_clean BC
ON WLE.Row_ID = BC.Row_ID
SET WLE.BMI = BC.avg_bmi;

# new percentageexpenditure
UPDATE worldlifexpectancy
SET percentageexpenditure = ROUND(health_expenditure/GDP*100.0,2);

SET SQL_SAFE_UPDATES = 1;

# EDA

/* 1. **Basic Descriptive Statistics**: 
   - Query to get the avg, mean, median, minimum, and maximum of the `Lifeexpectancy` for each `Country`.
   */
WITH ranked_data as (
  SELECT Country
   , Lifeexpectancy
    , ROW_NUMBER() over (partition by Country order by Lifeexpectancy) as STT
    , count(Lifeexpectancy) over (partition by Country) as SoLuong 
  from worldlifexpectancy
),
median as (
  select Country
  , Lifeexpectancy 
  from ranked_data 
  where STT in (floor((SoLuong+1)/2), ceil((SoLuong+1)/2))
)
, median_country AS
(
select Country
, ROUND(avg(Lifeexpectancy),2) AS median
from median
GROUP BY country
)

SELECT worldlifexpectancy.Country
, ROUND(AVG(worldlifexpectancy.Lifeexpectancy),2) AS average
, MIN(worldlifexpectancy.Lifeexpectancy) AS minimum
, MAX(worldlifexpectancy.Lifeexpectancy) AS maximum
, median_country.median AS median
FROM worldlifexpectancy
JOIN median_country
ON median_country.Country = worldlifexpectancy.Country
GROUP BY Country;

/* 2. **Trend Analysis**:
   - Query to find the trend of `Lifeexpectancy` over the years for a specific country (e.g., Afghanistan). 
   */

WITH normalized_data AS 
(
	SELECT Country
	, RANK() OVER (PARTITION BY Country ORDER BY Year) AS year_position -- x
	, 1.0 * Lifeexpectancy / AVG(Lifeexpectancy) OVER (PARTITION BY Country) AS normalized_expectancy -- y
	 FROM worldlifexpectancy
	)
 ,
stats AS 
(
  SELECT Country
		, AVG(year_position) AS year_avg
        , AVG(normalized_expectancy) AS normalized_expectancy_avg
	FROM normalized_data
	GROUP BY Country
)
, slope_data AS
(
	SELECT Country,
		  SUM((year_position - year_avg) * (normalized_expectancy - normalized_expectancy_avg)) /
			 (1.0 * SUM((year_position - year_avg) * (year_position - year_avg))) AS slope
	FROM normalized_data INNER JOIN stats USING (Country)
	GROUP BY Country
	ORDER BY ABS(slope) DESC
)

SELECT DISTINCT slope_data.Country
, worldlifexpectancy.Status
, slope_data.slope
, CASE
	WHEN slope_data.slope > 0 THEN 'increase'
    WHEN slope_data.slope <0 THEN 'decrease'
    ELSE 'not enough data to detect'
END AS trend
FROM slope_data
JOIN worldlifexpectancy 
ON worldlifexpectancy.Country = slope_data.Country
ORDER BY slope_data.Country;

/*
3. **Comparative Analysis**:
   - Query to compare the average `Lifeexpectancy` between `Developed` and `Developing` countries for the latest available year.
=> Citizen in developed country averagely live 11 years longer than those in developing country
*/
SELECT DISTINCT Status
, FIRST_VALUE(lastest) OVER (PARTITION BY Status ORDER BY Year DESC) AS lastest_avg_expectancy
FROM
(
SELECT Status
, Year
, ROUND(AVG(Lifeexpectancy) OVER (PARTITION BY Status, Year ORDER BY Year DESC),0)  AS lastest
FROM worldlifexpectancy
ORDER BY Year DESC
) al;

/*
4. **Mortality Analysis**:
   - Query to calculate the correlation between `AdultMortality` and `Lifeexpectancy` for all countries. 
*/

# correlation for all countries = -0.893 =>highly negatively correlated; the higher AdultMorality the lower LifeExxpectancy

WITH stats_avg AS
(
SELECT AVG(AdultMortality) AS avg_x -- 187.1974
, AVG(Lifeexpectancy)  AS avg_y -- 69.224881
FROM worldlifexpectancy
)
, stats_full AS
(SELECT AdultMortality-(SELECT avg_x FROM stats_avg) AS xi
, Lifeexpectancy-(SELECT avg_y FROM stats_avg) AS yi
, (SELECT avg_x FROM stats_avg) AS avg_x
, (SELECT avg_y FROM stats_avg) AS avg_y
FROM worldlifexpectancy
)
, stats_corr AS
(
SELECT SUM(xi*yi) AS upper_ratio
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio
FROM stats_full
)

SELECT upper_ratio/down_ratio AS corr_mortality_lifeexpectancy
FROM stats_corr;

/*
5. **Impact of GDP**:
   - Query to find the average `Lifeexpectancy` of countries grouped by their GDP ranges (e.g., low, medium, high which is you decided).
*/
CREATE TEMPORARY TABLE gdp_cat AS
(
WITH last_gdp AS
(
SELECT DISTINCT Country
, FIRST_VALUE(GDP) OVER (PARTITION BY Country ORDER BY Year DESC) AS lastest_GDP
FROM worldlifexpectancy
)
, cal_stats AS
(
SELECT Country
, lastest_GDP
, (SELECT SUM(lastest_GDP) FROM last_gdp) AS total_world
FROM last_gdp
)
, pareto AS(
SELECT Country
, ROUND(lastest_GDP/total_world*100.0,4) AS percent_in_total
FROM cal_stats
)
, rolling_pareto AS
(
SELECT *
, SUM(percent_in_total) OVER (ORDER BY percent_in_total DESC) AS rolling_percent
, ROW_NUMBER() OVER (ORDER BY percent_in_total DESC) AS counting
FROM pareto
ORDER BY percent_in_total DESC
)

SELECT Country
, CASE 
	WHEN rolling_percent <71 THEN 'high'
    WHEN rolling_percent <90 THEN 'medium'
    ELSE 'low'
END AS GDP_catergoized 
FROM rolling_pareto
);

SELECT GC.GDP_catergoized
, AVG(WLE.Lifeexpectancy) AS average_life_expectancy
FROM worldlifexpectancy WLE
JOIN gdp_cat GC
ON WLE.country = GC.country
GROUP BY GC.GDP_catergoized;

# country with high GDP has higher life_expectancy

/*
6. **Disease Impact**:
   - Query to analyze the impact of `Measles` and `Polio` on `Lifeexpectancy`. 
   Calculate average life expectancy for countries with high and low incidence rates of these diseases.
*/
DROP TABLE IF EXISTS measles_cat, polio_cat;
CREATE TEMPORARY TABLE measles_cat AS
(
WITH last_measles AS
(
SELECT DISTINCT Country
, AVG(Measles) OVER (PARTITION BY Country ) AS lastest_Measles
FROM worldlifexpectancy
)
, cal_stats AS
(
SELECT DISTINCT Country
, lastest_Measles
, (SELECT SUM(lastest_Measles) FROM last_measles) AS total_world
FROM last_measles
)
, pareto AS(
SELECT Country
, ROUND(lastest_Measles/total_world*100.0,4) AS percent_in_total
FROM cal_stats
)
, rolling_pareto AS
(
SELECT *
, SUM(percent_in_total) OVER (ORDER BY percent_in_total DESC) AS rolling_percent
, ROW_NUMBER() OVER (ORDER BY percent_in_total DESC) AS counting
FROM pareto
ORDER BY percent_in_total DESC
)

SELECT *
, CASE 
	WHEN rolling_percent <81 THEN 'high'
    ELSE 'low'
END AS Measles_catergoized 
FROM rolling_pareto
);

------------- 
CREATE TEMPORARY TABLE polio_cat AS
(
WITH last_polio AS
(
SELECT DISTINCT Country
, AVG(polio) OVER (PARTITION BY Country) AS lastest_polio
FROM worldlifexpectancy
)
, cal_stats AS
(
SELECT DISTINCT Country
, lastest_polio
, (SELECT SUM(lastest_polio) FROM last_polio) AS total_world
FROM last_polio
)
, pareto AS(
SELECT Country
, ROUND(lastest_polio/total_world*100.0,4) AS percent_in_total
FROM cal_stats
)
, rolling_pareto AS
(
SELECT *
, SUM(percent_in_total) OVER (ORDER BY percent_in_total DESC) AS rolling_percent
, ROW_NUMBER() OVER (ORDER BY percent_in_total DESC) AS counting
FROM pareto
ORDER BY percent_in_total DESC
)

SELECT *
, CASE 
	WHEN rolling_percent < 61 THEN 'high'
    ELSE 'low'
END AS polio_catergoized 
FROM rolling_pareto
);

SELECT PC.polio_catergoized
, MC.Measles_catergoized
, AVG(WLE.Lifeexpectancy) AS average_life_expectancy
FROM worldlifexpectancy WLE
JOIN measles_cat MC
ON WLE.country = MC.country
JOIN polio_cat PC
ON WLE.country = PC.country
GROUP BY PC.polio_catergoized, MC.Measles_catergoized
ORDER BY average_life_expectancy DESC ;

# Measles have a bigger impact on life expectancy

/*
7. **Schooling and Health**:
   - Query to determine the relationship between `Schooling` and `Lifeexpectancy`. 
   Find countries with the highest and lowest schooling and their respective life expectancies.
*/

-- correlation schooling vs lifeexpectancy = 0.76 => life expentancy increase as schooling increase

WITH stats_avg AS
(
SELECT AVG(Schooling) AS avg_x -- 187.1974
, AVG(Lifeexpectancy)  AS avg_y -- 69.224881
FROM worldlifexpectancy
)
, stats_full AS
(SELECT Schooling-(SELECT avg_x FROM stats_avg) AS xi
, Lifeexpectancy-(SELECT avg_y FROM stats_avg) AS yi
, (SELECT avg_x FROM stats_avg) AS avg_x
, (SELECT avg_y FROM stats_avg) AS avg_y
FROM worldlifexpectancy
)
, stats_corr AS
(
SELECT SUM(xi*yi) AS upper_ratio
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio
FROM stats_full
)

SELECT upper_ratio/down_ratio AS corr_Schooling_lifeexpectancy
FROM stats_corr;

-- min, max schooling and avg life expectancy
SELECT Country
, Schooling
, Lifeexpectancy
FROM worldlifexpectancy
WHERE Schooling = (SELECT MIN(Schooling) FROM worldlifexpectancy)
OR Schooling = (SELECT MAX(Schooling) FROM worldlifexpectancy);

# Australia with max schooling is 72% higher in life expectancy than lowest-schooling country (Niger)

/* 8. **BMI Trends**:
   - Query to find the average BMI trend over the years for a particular country. 
   */

WITH normalized_data AS 
(
	SELECT Country
	, RANK() OVER (PARTITION BY Country ORDER BY Year) AS year_position -- x
	, 1.0 * BMI / AVG(BMI) OVER (PARTITION BY Country) AS normalized_BMI -- y
	 FROM worldlifexpectancy
	)
 ,
stats AS 
(
  SELECT Country
		, AVG(year_position) AS year_avg
        , AVG(normalized_BMI) AS normalized_BMI_avg
	FROM normalized_data
	GROUP BY Country
)
, slope_data AS
(
	SELECT Country,
		  SUM((year_position - year_avg) * (normalized_BMI - normalized_BMI_avg)) /
			 (1.0 * SUM((year_position - year_avg) * (year_position - year_avg))) AS slope
	FROM normalized_data INNER JOIN stats USING (Country)
	GROUP BY Country
	ORDER BY ABS(slope) DESC
)

SELECT DISTINCT slope_data.Country
, slope_data.slope
, CASE
	WHEN slope_data.slope > 0 THEN 'increase'
    WHEN slope_data.slope <0 THEN 'decrease'
    ELSE 'not enough data to detect'
END AS trend
FROM slope_data
JOIN worldlifexpectancy 
ON worldlifexpectancy.Country = slope_data.Country
ORDER BY slope_data.Country;
   
/* 9. **Infant Mortality**:
   - Query to find the average number of `infantdeaths` and `under-fivedeaths` for countries with the highest and lowest life expectancies.
   */
   
WITH avg_worldlifexpectancy AS
(
SELECT Country
, AVG(Lifeexpectancy) AS avgLifeexpectancy
FROM worldlifexpectancy
GROUP BY Country
)
, top_countries AS
(
SELECT *
, CASE
	WHEN avgLifeexpectancy = (SELECT MIN(avgLifeexpectancy) FROM avg_worldlifexpectancy) THEN 'lowest_expectancy'
    ELSE 'highest_expectancy'
END AS ranking
FROM avg_worldlifexpectancy
WHERE avgLifeexpectancy = (SELECT MIN(avgLifeexpectancy) FROM avg_worldlifexpectancy)
OR avgLifeexpectancy = (SELECT MAX(avgLifeexpectancy) FROM avg_worldlifexpectancy)
)

SELECT WLE.Country
, TC.ranking
, AVG(WLE.infantdeaths) AS avg_infants_death
, AVG(WLE.under_fivedeaths) AS avg_under_five
FROM worldlifexpectancy WLE
JOIN top_countries TC
ON WLE.Country = TC.Country
GROUP BY WLE.Country;

   
/*
10. **Rolling Average of Adult Mortality**:
    - Query to calculate the rolling average of `AdultMortality` over a 5-year window for each country. 
    This will help in understanding the trend and smoothing out short-term fluctuations.
*/


SELECT Country
, Year 
, CASE 
	WHEN Year < FIRST_VALUE(Year) OVER (PARTITION BY Country ORDER BY Year ASC)+4
		THEN 0
	ELSE AVG(AdultMortality) OVER (PARTITION BY Country ORDER BY Year ASC ROWS 5 PRECEDING) 
END AS moving_avg
FROM worldlifexpectancy ;


/*
11. **Impact of Healthcare Expenditure**:
    - Query to find the correlation between `percentageexpenditure` (healthcare expenditure) and `Lifeexpectancy`. 
    Higher healthcare spending might correlate with higher life expectancy.
*/

# Correlation = 0.22 => positive low relation, higher healthcare spent, higher life expectancy

WITH stats_avg AS
(
SELECT AVG(percentageexpenditure) AS avg_x -- 187.1974
, AVG(Lifeexpectancy)  AS avg_y -- 69.224881
FROM worldlifexpectancy
)
, stats_full AS
(SELECT percentageexpenditure-(SELECT avg_x FROM stats_avg) AS xi
, Lifeexpectancy-(SELECT avg_y FROM stats_avg) AS yi
, (SELECT avg_x FROM stats_avg) AS avg_x
, (SELECT avg_y FROM stats_avg) AS avg_y
FROM worldlifexpectancy
)
, stats_corr AS
(
SELECT SUM(xi*yi) AS upper_ratio
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio
FROM stats_full
)

SELECT upper_ratio/down_ratio AS corr_percentageexpenditure_lifeexpectancy
FROM stats_corr;

/*
12. **BMI and Health Indicators**:
    - Query to find the correlation between `BMI` and other health indicators like `Lifeexpectancy` and `AdultMortality`.
    Analyze the impact of BMI on overall health.
*/

# BMI - life: 0.57
# BMI - AdultMorality: -0.49 => BMI tăng thì tuổi thọ kéo dài, tỷ lệ tử vong giảm, mức tương quan trung bình
WITH stats_avg AS
(
SELECT AVG(BMI) AS avg_x 
, AVG(Lifeexpectancy)  AS avg_y 
, AVG(AdultMortality)  AS avg_z 
FROM worldlifexpectancy
)
, stats_full AS
(SELECT BMI-(SELECT avg_x FROM stats_avg) AS xi
, Lifeexpectancy-(SELECT avg_y FROM stats_avg) AS yi
, AdultMortality-(SELECT avg_z FROM stats_avg) AS zi
, (SELECT avg_x FROM stats_avg) AS avg_x
, (SELECT avg_y FROM stats_avg) AS avg_y
, (SELECT avg_z FROM stats_avg) AS avg_z
FROM worldlifexpectancy
)
, stats_corr AS
(
SELECT SUM(xi*yi) AS upper_ratio_xy
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio_xy
, SUM(xi*zi) AS upper_ratio_xz
, SQRT(SUM(xi*xi)*SUM(zi*zi)) AS down_ratio_xz
FROM stats_full
)

SELECT upper_ratio_xy/down_ratio_xy AS corr_BMI_lifeexpectancy
, upper_ratio_xz/down_ratio_xz AS corr_BMI_AdultMortality
FROM stats_corr;

/*
13. **GDP and Health Outcomes**:
    - Query to analyze how `GDP` influences health outcomes such as `Lifeexpectancy`, `AdultMortality`, and `infantdeaths`. 
    Compare high GDP and low GDP countries.
*/

# overall GDP has medium correlation to `Lifeexpectancy` (0.55), `AdultMortality`(-0.46), and  low correlation `infantdeaths`(-0.12)
# GDP increase => `Lifeexpectancy` increase, `AdultMortality` decrease, and `infantdeaths` decrease
WITH stats_avg AS
(
SELECT AVG(GDP) AS avg_x 
, AVG(Lifeexpectancy)  AS avg_y 
, AVG(AdultMortality)  AS avg_z 
, AVG(infantdeaths)  AS avg_t
FROM worldlifexpectancy
)
, stats_full AS
(SELECT GDP-(SELECT avg_x FROM stats_avg) AS xi
, Lifeexpectancy-(SELECT avg_y FROM stats_avg) AS yi
, AdultMortality-(SELECT avg_z FROM stats_avg) AS zi
, infantdeaths-(SELECT avg_t FROM stats_avg) AS ti
, (SELECT avg_x FROM stats_avg) AS avg_x
, (SELECT avg_y FROM stats_avg) AS avg_y
, (SELECT avg_z FROM stats_avg) AS avg_z
, (SELECT avg_t FROM stats_avg) AS avg_t
FROM worldlifexpectancy
)
, stats_corr AS
(
SELECT SUM(xi*yi) AS upper_ratio_xy
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio_xy
, SUM(xi*zi) AS upper_ratio_xz
, SQRT(SUM(xi*xi)*SUM(zi*zi)) AS down_ratio_xz
, SUM(xi*ti) AS upper_ratio_xt
, SQRT(SUM(xi*xi)*SUM(ti*ti)) AS down_ratio_xt
FROM stats_full
)

SELECT upper_ratio_xy/down_ratio_xy AS corr_GDP_lifeexpectancy
, upper_ratio_xz/down_ratio_xz AS corr_GDP_AdultMortality
, upper_ratio_xt/down_ratio_xt AS corr_GDP_infantdeaths
FROM stats_corr;

# by GDP_catergoized 
WITH stats_full AS
(SELECT GC.GDP_catergoized
, WLE.GDP-AVG(GDP) OVER (PARTITION BY GC.GDP_catergoized) AS xi
, Lifeexpectancy-AVG(Lifeexpectancy) OVER (PARTITION BY GC.GDP_catergoized) AS yi
, AdultMortality-AVG(AdultMortality) OVER (PARTITION BY GC.GDP_catergoized) AS zi
, infantdeaths-AVG(infantdeaths) OVER (PARTITION BY GC.GDP_catergoized) AS ti
FROM worldlifexpectancy WLE
JOIN gdp_cat GC
ON WLE.Country = GC.Country
	AND GC.GDP_catergoized IN ('high','low')
)
, stats_corr AS
(
SELECT GDP_catergoized
, SUM(xi*yi) AS upper_ratio_xy
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio_xy
, SUM(xi*zi) AS upper_ratio_xz
, SQRT(SUM(xi*xi)*SUM(zi*zi)) AS down_ratio_xz
, SUM(xi*ti) AS upper_ratio_xt
, SQRT(SUM(xi*xi)*SUM(ti*ti)) AS down_ratio_xt
FROM stats_full
GROUP BY GDP_catergoized
)

SELECT GDP_catergoized
, upper_ratio_xy/down_ratio_xy AS corr_GDP_lifeexpectancy
, upper_ratio_xz/down_ratio_xz AS corr_GDP_AdultMortality
, upper_ratio_xt/down_ratio_xt AS corr_GDP_infantdeaths
FROM stats_corr;WITH stats_full AS
(SELECT GC.GDP_catergoized
, WLE.GDP-AVG(GDP) OVER (PARTITION BY GC.GDP_catergoized) AS xi
, Lifeexpectancy-AVG(Lifeexpectancy) OVER (PARTITION BY GC.GDP_catergoized) AS yi
, AdultMortality-AVG(AdultMortality) OVER (PARTITION BY GC.GDP_catergoized) AS zi
, infantdeaths-AVG(infantdeaths) OVER (PARTITION BY GC.GDP_catergoized) AS ti
FROM worldlifexpectancy WLE
JOIN gdp_cat GC
ON WLE.Country = GC.Country
	AND GC.GDP_catergoized IN ('high','low')
)
, stats_corr AS
(
SELECT GDP_catergoized
, SUM(xi*yi) AS upper_ratio_xy
, SQRT(SUM(xi*xi)*SUM(yi*yi)) AS down_ratio_xy
, SUM(xi*zi) AS upper_ratio_xz
, SQRT(SUM(xi*xi)*SUM(zi*zi)) AS down_ratio_xz
, SUM(xi*ti) AS upper_ratio_xt
, SQRT(SUM(xi*xi)*SUM(ti*ti)) AS down_ratio_xt
FROM stats_full
GROUP BY GDP_catergoized
)

SELECT GDP_catergoized
, upper_ratio_xy/down_ratio_xy AS corr_GDP_lifeexpectancy
, upper_ratio_xz/down_ratio_xz AS corr_GDP_AdultMortality
, upper_ratio_xt/down_ratio_xt AS corr_GDP_infantdeaths
FROM stats_corr;
/*
14. **Subgroup Analysis of Life Expectancy**:
    - Query to find the average `Lifeexpectancy` for specific subgroups, such as countries in different continents or regions. 
    This can help in identifying regional health disparities.
*/
