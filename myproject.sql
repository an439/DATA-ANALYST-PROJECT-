SELECT gender,AVG(age),max(age),count(age)
FROM employee_demographics
GROUP BY gender;


SELECT *
FROM employee_demographics
JOIN employee_salary;

SELECT dem.employee_id,age
FROM employee_demographics AS dem
INNER JOIN employee_salary AS sal
      ON dem.employee_id=sal.employee_id;
      
      
      SELECT first_name,last_name
      FROM employee_demographics
      UNION 
      SELECT first_name,last_name
      
	  SELECT  first_name,last_name, 'old man' as Lable
      FROM employee_demographics
      WHERE age>40 and gender= 'Male'
      UNION
      SELECT first_name,last_name, 'old lady' as Lable
      FROM employee_demographics
      WHERE age>40 and gender= 'Female'
      UNION
      SELECT first_name,last_name, 'high paid' as Lable
      FROM employee_salary
      WHERE salary>40000
      ;
      
      -- STRING FUNCTIONS
      
      SELECT first_name,upper(first_name)
      FROM employee_demographics;
      
      SELECT first_name,LENGTH(first_name)
      FROM employee_demographics;
      
      -- CASE STATEMENT
      
      SELECT first_name,
      last_name,
      age,
      CASE
           WHEN age BETWEEN 30 and 50 THEN 'OLD'
           WHEN age<=30 THEN 'Young'
      END AS Age_Bracket
      FROM employee_demographics;
      
      
      
      
      
      
      SELECT dem.employee_id, dem.first_name,dem.last_name,avg(salary) OVER(PARTITION BY gender),
      SUM(salary) OVER(PARTITION BY gender ORDER BY dem.employee_id) AS Rolling_total,
      ROW_NUMBER() OVER(PARTITION BY gender)
      FROM employee_demographics dem
      JOIN employee_salary  sal
           ON dem.employee_id=sal.employee_id;
           
           
           
           
           
           
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
      
      
    -- REMOVING UNNECESSARY COLUMN 
    
	 DELETE
     FROM layoffs_staging2
     WHERE total_laid_off IS NULL
     AND percentage_laid_off IS NULL;
     
    SELECT *
   FROM layoffs_staging2
	WHERE total_laid_off IS NULL
     AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;