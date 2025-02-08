#Deleting duplicates
select *
from layoffs
;

#Here we are creating another table similar to the layoffs table.

create table layoffs_staging
like layoffs
;

#here we have created a table which shows all the column names from the layoffs table.
select *
from layoffs_staging
;

#Now we are going to transfer the data from the layoffs table to the layoffs_staging table.alter

insert layoffs_staging
select *
from layoffs
;

select *
from layoffs_staging
;

#Now we are going to look for duplicates in the staging dataset

select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging
;

#As you can see the above query assigns the number 1 to all unique values and numbers greater than 1 to
#their duplicates. 
#You can also see we have mentioned `date` in these quotes because date is actually a function in sql. 

#Lets put this query in a cte so it would be easy to call it whenever we want.alter

with duplicate_cte as 
(
select *,
row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1
;

#Now in order to remove or delete the duplicate values we will create another table

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
;

insert into layoffs_staging2
select *,
row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num
from layoffs_staging
;

select *
from layoffs_staging2
where row_num > 1
;

delete
from layoffs_staging2
where row_num >1
;

select *
from layoffs_staging2
where row_num > 1
;

#Standardizing the data

select trim(company)
from layoffs_staging2
;
#Trim basically gets rid of all the white space on the left of the data in the table

update layoffs_staging2
set company = trim(company)
;

select distinct industry
from layoffs_staging2
order by 1
;

select *
from layoffs_staging2
where industry like 'Crypto%'
;

#From the above query we see that there are three different inputs for crypto

#Now we are going to set them all to Crypto

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%'
;

select *
from layoffs_staging2
where industry like 'Crypto%'
;

select distinct country
from layoffs_staging2
order by 1
;
#Similarly for united states 

select *
from layoffs_staging2
where country like 'United States%'
;

#Following is an advanced function to get rid of typos like these in the dataset using trailing
select country, trim(trailing '.' from country)
from layoffs_staging2
;

update layoffs_staging2
set country = 'United States'
where country like 'United States%'
;

#Also further down the line we need to do data exploratory hence for that we need a correct format
#of the date to be presented.

select `date`
from layoffs_staging2
;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2
;

#The above query converts the string date to int date however it follows a format, where it takes 
#two arguments, one the column name i.e. date and second the format for the date to be dsiplayed
#which is a specific format in MySQL i.e. '%m/%d/Y'

#Now we will update the table as well

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y')
;

select *
from layoffs_staging2
;

#Now we are going to change the data type of the column date, i.e. from text to int.

alter table layoffs_staging2
modify column `date` date
;

#Never run the above query on the original dataset, always do it on a duplicated column so that if 
#anything goes wrong we dont have to re-do the whole thing.

#WORKING WITH NULL AND BLANK VALUES

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null
;

select *
from layoffs_staging2
where total_laid_off is null or percentage_laid_off is null
;

select industry
from layoffs_staging2
;

select *
from layoffs_staging2
where industry is null
or industry = ''
;

select * 
from layoffs_staging2
where company like "Bally's Interactive"
;

#Here we are converting the blank spaces to null because further down the line when we try to
#populate the industry of the companies which are balnk or null in some places it does not render
#the blank space ones hence coverting them all to null makes it easier for the system to comprehend. 
update layoffs_staging2
set industry = null
where industry = ''
;

#Here we are joining the same table to match the null values in the industry column named table 1 to the
#not null values in the industry column named table 2 to populates these empty values. 
select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null
;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null
;

select *
from layoffs_staging2
;

#Now we are deleting the rows that have null or blank value beacause we believe they won't add any 
#value to the data exploratory we are going to do ahead. 

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

#We will also get rid of the row_num column beacuse we don't need it anymore since we have gotten rid
#of the duplicate values which we used it for.

alter table layoffs_staging2
drop column row_num
;

select *
from layoffs_staging2
;

#Exploratory Data Analysis

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc
;

select company, total_laid_off
from layoffs_staging2
order by 2 desc
;

select *
from layoffs_staging2
;

select substring( `date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring( `date`, 1,7) is not null
group by `month`
order by 1 asc
;

with rolling_total as
(
select substring( `date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring( `date`, 1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off, sum(total_off) over(order by `month`) as month_wise_total_off
from rolling_total
;

with rolling_total2 as
(
select country, sum(total_laid_off) as total_off
from layoffs_staging2
group by country
order by 1 desc
)
select country, total_off, sum(total_off) over(order by country) as country_wise_total_off
from rolling_total2
;

with rolling_total3 as
(
select country, company, sum(total_laid_off) as total_off
from layoffs_staging2
group by country, company
order by 1 desc
)
select country, company, total_off, sum(total_off) over(order by country, company) as country_wise_total_off
from rolling_total3
;

with yearly_layoff(country, company, years, total_laid_off) as
(
select country, company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by country, company, year(`date`)
), company_yearly_ranking as 
(select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from yearly_layoff
where years is not null
)
select *
from company_yearly_ranking
where ranking <= 5
;












