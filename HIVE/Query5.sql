--Q5-Find the most popular top 10 job positions for H1B visa applications for each year?

CREATE TABLE h1b_1(s_no int,case_status string, employer_name string, soc_name string, job_title string, full_time_position string,prevailing_wage int,year string, worksite string, longitute double, latitute double )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\""
) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/shubha/PROJECT/h1b.csv' OVERWRITE INTO TABLE h1b_1;


 describe h1b_1;

select job_title,count(job_title) as temp1, year from h1b_1 where year = '2011' group by year, job_title order by temp1 desc limit 10;
 select job_title,count(job_title) as temp1, year from h1b_1 where year = '2012' group by year, job_title order by temp1 desc limit 10;
select job_title,count(job_title) as temp1, year from h1b_1 where year = '2013' group by year, job_title order by temp1 desc limit 10;
select job_title,count(job_title) as temp1, year from h1b_1 where year = '2014' group by year, job_title order by temp1 desc limit 10;
select job_title,count(job_title) as temp1, year from h1b_1 where year = '2015' group by year, job_title order by temp1 desc limit 10;
select job_title,count(job_title) as temp1, year from h1b_1 where year = '2016' group by year, job_title order by temp1 desc limit 10;
