SELECT * FROM world_layoffs.layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging ;

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date' ) AS row_num
FROM layoffs_staging 
;

WITH duplicate_cte AS
(SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging 
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
'date', stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging; 

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'united states%'
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country =  TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%'
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

SELECT  `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'
;

 SELECT t1.company, t1.industry, t2.industry
 FROM layoffs_staging2 AS t1
 JOIN  layoffs_staging2 AS t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
where industry = '';


UPDATE layoffs_staging2 as t1
JOIN layoffs_staging2 as t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging ;

select *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


select company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
order by 2 desc
;

SELECT min( `date`), max( `date`)
FROM layoffs_staging2;

select industry, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
order by 2 desc;

WITH Rolling_total as 
(
select substring( `date`,1,7) AS  `month`, SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
select  `month`, total_off,
SUM(total_off) OVER(ORDER BY `month`) as rolling_total
FROM Rolling_total ;


SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
group by company, year(`date`)
order by 3 desc;

WITH Company_year (company, years, total_laid_off) AS
(
SELECT company, year(`date`), SUM(total_laid_off)
FROM layoffs_staging2
group by company, year(`date`)
),
Company_year_rank AS
(
SELECT *,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) as Ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5
;
