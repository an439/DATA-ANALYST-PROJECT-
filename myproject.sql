 -- Data Cleaning Project  
        
SELECT * 
FROM layoffs;
          
          -- CREATING ANOTHER TABLE LIKE LAYOFFS 
          
          CREATE TABLE layoffs_staging
          LIKE layoffs;
          
          -- INSERTING VALUES OF LAYOFF IN NEW TABLE
          INSERT layoffs_staging
          SELECT * 
          FROM layoffs;
          
SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off) AS row_num
FROM layoffs_staging;

          -- For Finding the Duplicates
          
  WITH duplicate_cte AS 
  (
  SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
  )
  SELECT *
  FROM duplicate_cte
  WHERE row_num > 1 ;
  
  
  
  SELECT *
  FROM layoffs_staging
  WHERE company='oda';
  
           
     SELECT `layoffs_staging2`.`company`,
    `layoffs_staging2`.`location`,
    `layoffs_staging2`.`industry`,
    `layoffs_staging2`.`total_laid_off`,
    `layoffs_staging2`.`percentage_laid_off`,
    `layoffs_staging2`.`stage`,
    `layoffs_staging`.`country`,
    `layoffs_staging`.`funds_raised_millions`,
    `layoff_staging2`. `row_num`
FROM `my_layoffs`.`layoffs_staging2`;
      
           
         
         CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;


-- STANDERDIZING THE DATA 

UPDATE layoffs_staging2
SET COMPANY = TRIM(company);

SELECT distinct(industry)
FROM layoffs_staging2
ORDER BY 1 ;

UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry LIKE 'Crypto%';
           
SELECT distinct(industry)
FROM layoffs_staging2
ORDER BY 1 ;          
           
           SELECT  DISTINCT country ,  TRIM(TRAILING '.' FROM country)
           FROM layoffs_staging2
           ORDER BY 1 ;   
           
          UPDATE layoffs_staging2
          SET country = TRIM(TRAILING '.' FROM country)
          WHERE country LIKE 'United States%';
          
          SELECT  DISTINCT country 
           FROM layoffs_staging2
           ORDER BY 1 ;   
           
           
      UPDATE layoffs_staging2 
      SET industry = NULL
      WHERE industry = ' ';
           
      UPDATE layoffs_staging2 t1
      JOIN layoffs_staging2  t2 
          ON t1.company=t2.company
       SET t1.industry = t2.industry     
          WHERE (t1.industry IS NULL OR t1.industry = ' ')
          AND t2.industry IS NOT NULL;
      
      
    -- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- now that's taken care of:
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- --------------------------------------------------
-- we also need to look at 

SELECT *
FROM world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_staging2;

-- we can use str to date to update this field
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
FROM world_layoffs.layoffs_staging2;





-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values




-- 4. remove any columns and rows we need to

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM world_layoffs.layoffs_staging2;
