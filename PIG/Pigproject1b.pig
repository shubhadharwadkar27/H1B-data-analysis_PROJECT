--Find top 5 job titles who are having highest growth in applications

REGISTER /home/shubha/PROJECT/piggybank.jar;
DEFINE CSV_Storage org.apache.pig.piggybank.storage.CSVExcelStorage(); 
L1 = load '/home/shubha/PROJECT/h1b.csv' using CSV_Storage() as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);

group1 = foreach L1 generate $1,$4,$7;
g1 = FILTER group1 by $2 =='2011';
g2011 =  group g1 by (job_title);
count2011 = foreach g2011 generate group, COUNT($1);
--dump count2011;

g2 = FILTER group1 by $2 == '2012';
g2012 = group g2 by (job_title);
count2012 = foreach g2012 generate group, COUNT($1);
--dump count2012;

g3 = FILTER group1 by $2 == '2013';
g2013 = group g3 by (job_title);
count2013 = foreach g2013 generate group, COUNT($1);

g4 = FILTER group1 by $2 == '2014';
g2014 = group g4 by (job_title);
count2014 = foreach g2014 generate group, COUNT($1);

g5 = FILTER group1 by $2 == '2015';
g2015 = group g2 by (job_title);
count2015 = foreach g2015 generate group, COUNT($1);

g6 = FILTER group1 by $2 == '2016';
g2016 = group g2 by (job_title);
count2016 = foreach g2016 generate group, COUNT($1);
--describe count2016;


group2 = join count2011 by $0, count2012 by $0, count2013 by $0, count2014 by $0, count2015 by $0, count2016 by $0;
--dump group2;

group3 = foreach group2 generate $0,$1,$3,$5,$7,$9,$11;
--dump group3;

growth = foreach group3 generate $0, (float)(($2-$1)*100/$1),(float)(($3-$2)*100/$2),(float)(($4-$3)*100/$4),(float)(($5-$4)*100/$4),(float)(($6-$5)*100/$5);
avg_growth = foreach growth generate $0, (float)(($1+$2+$3+$4+$5)/5);
--dump avg_growth;

top5 = order avg_growth by $1 DESC;
top5_op = limit top5 5;
dump top5_op;



