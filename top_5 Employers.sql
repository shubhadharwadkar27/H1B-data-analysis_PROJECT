##Q4--- top5 employers who file most petitions every year


CREATE TABLE h1b_1(s_no int,case_status string, employer_name string, soc_name string, job_title string, full_time_position string,prevailing_wage int,year string, worksite string, longitute double, latitute double )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\""
) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/shubha/PROJECT/sample.csv' OVERWRITE INTO TABLE h1b_1;


 describe h1b_1;

select employer_name, count(case_status) as temp, year from h1b_1 where case_status ='CERTIFIED' and year= '2011' group by year,employer_name order by temp desc limit 5;
select employer_name, count(case_status) as temp, year from h1b_1 where case_status ='CERTIFIED' and year= '2012' group by year,employer_name order by temp desc limit 5;
select employer_name, count(case_status) as temp, year from h1b_1 where case_status ='CERTIFIED' and year= '2013' group by year,employer_name order by temp desc limit 5;
select employer_name, count(case_status) as temp, year from h1b_1 where case_status ='CERTIFIED' and year= '2014' group by year,employer_name order by temp desc limit 5;
select employer_name, count(case_status) as temp, year from h1b_1 where case_status ='CERTIFIED' and year= '2015' group by year,employer_name order by temp desc limit 5;
select employer_name, count(case_status) as temp, year from h1b_1 where case_status ='CERTIFIED' and year= '2016d' group by year,employer_name order by temp desc limit 5;
