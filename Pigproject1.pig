//Is the number of petitions with Data Engineer job title increasing over time?


REGISTER /home/shubha/PROJECT/piggybank.jar;
DEFINE CSV_Storage org.apache.pig.piggybank.storage.CSVExcelStorage(); 
L1 = load '/home/shubha/PROJECT/h1b.csv' using CSV_Storage() as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);
group1 = limit L1 5;
--dump group1;

group2 = foreach L1 generate $4,$7;
abc1 = FILTER group2 BY $0 == 'DATA ENGINEER';
--dump abc1;





group3 = group abc1 by $1;

group4 = foreach group3 generate group, COUNT(abc1.$0);
--dump group4; 
group2011 = filter group4 by $0=='2011';
--dump group2011;
group2012 = filter group4 by $0=='2012';
group2013 = filter group4 by $0=='2013';
group2014 = filter group4 by $0=='2014';
group2015 = filter group4 by $0=='2015';
group2016 = filter group4 by $0=='2016';

data = foreach group2012 generate group2011.$1, group2012.$1, group2013.$1, group2014.$1, group2015.$1, group2016.$1;
dump data;
percent = foreach data generate CONCAT((chararray)((float)(($1-$0)*100/$0)),'%'), CONCAT((chararray)((float)(($2-$1)*100/$1)),'%'), CONCAT((chararray)((float)(($3-$2)*100/$2)),'%'), CONCAT((chararray)((float)(($4-$3)*100/$3)),'%'), CONCAT((chararray)((float)(($5-$4)*100/$4)),'%'); 
dump percent;
percent1 = foreach percent generate FLATTEN(TOBAG(*));
dump percent1;


--output----
--(77.0%)
--(28.0%)
--(117.0%)
--(79.0%)
--(56.0%)

