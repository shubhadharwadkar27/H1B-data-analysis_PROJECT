--Find the percentage and the count of each case status on total applications for each year.

CREATE TABLE h1b_1(s_no int,case_status string, employer_name string, soc_name string, job_title string, full_time_position string,prevailing_wage int,year string, worksite string, longitute double, latitute double )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = "\""
) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/shubha/PROJECT/h1b.csv' OVERWRITE INTO TABLE h1b_1;

drop table totaltable;
create table totaltable(year string,cnt int);
insert overwrite table totaltable select year,count(case_status) as temp from h1b_1  group by year;

select a.case_status, 100*(count(a.case_status)/t.cnt), a.year from h1b_1  a,totaltable t where a.year ='2011' and t.year = '2011' group by a.case_status,a.year,t.cnt;
select a.case_status, 100*(count(a.case_status)/t.cnt), a.year from h1b_1  a,totaltable t where a.year ='2012' and t.year = '2012' group by a.case_status,a.year,t.cnt;
select a.case_status, 100*(count(a.case_status)/t.cnt), a.year from h1b_1  a,totaltable t where a.year ='2013' and t.year = '2013' group by a.case_status,a.year,t.cnt;
select a.case_status, 100*(count(a.case_status)/t.cnt), a.year from h1b_1  a,totaltable t where a.year ='2014' and t.year = '2014' group by a.case_status,a.year,t.cnt;
select a.case_status, 100*(count(a.case_status)/t.cnt), a.year from h1b_1  a,totaltable t where a.year ='2015' and t.year = '2015' group by a.case_status,a.year,t.cnt;
select a.case_status, 100*(count(a.case_status)/t.cnt), a.year from h1b_1  a,totaltable t where a.year ='2016' and t.year = '2016' group by a.case_status,a.year,t.cnt;


