--Q6 Find the average Prevailing Wage for each Job for each Year (take part time and full time separate)

CREATE TABLE h1b_1(s_no int,case_status string, employer_name string, soc_name string, job_title string, full_time_position string,prevailing_wage int,year string, worksite string, longitute double, latitute double )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\""
) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/shubha/PROJECT/h1b.csv' OVERWRITE INTO TABLE h1b_1;


 describe h1b_1;

select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2011" and full_time_position='Y' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2012" and full_time_position='Y' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2013" and full_time_position='Y' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2014" and full_time_position='Y' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2015" and full_time_position='Y' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2016" and full_time_position='Y' group by job_title,year;


select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2011" and full_time_position='N' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2012" and full_time_position='N' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2013" and full_time_position='N' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2014" and full_time_position='N' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2015" and full_time_position='N' group by job_title,year;
select job_title, AVG(prevailing_wage),year from h1b_1 where year = "2016" and full_time_position='N' group by job_title,year;

