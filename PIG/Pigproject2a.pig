--Which part of the US has the most Data Engineer jobs for each year?


REGISTER /home/shubha/PROJECT/piggybank.jar;
DEFINE CSV_Storage org.apache.pig.piggybank.storage.CSVExcelStorage(); 
L1 = load '/home/shubha/PROJECT/h1b.csv' using CSV_Storage() as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);



group2 = foreach L1 generate $4,$8,$7;
abc1 = FILTER group2 BY $0 == 'DATA ENGINEER';


abc2011 = FILTER abc1 BY $2 == '2011';
abc2012 = FILTER abc1 BY $2 == '2012';
abc2013 = FILTER abc1 BY $2 == '2013';
abc2014 = FILTER abc1 BY $2 == '2014';
abc2015 = FILTER abc1 BY $2 == '2015';
abc2016 = FILTER abc1 BY $2 == '2016';



abc3_2011 = group abc2011 by ($0,$1,$2);
abc3_2012 = group abc2012 by ($0,$1,$2);
abc3_2013 = group abc2013 by ($0,$1,$2);
abc3_2014 = group abc2014 by ($0,$1,$2);
abc3_2015 = group abc2015 by ($0,$1,$2);
abc3_2016 = group abc2016 by ($0,$1,$2);
--dump abc3;

group_2011 = foreach abc3_2011 generate group,COUNT(abc2011.$0);
group_2012 = foreach abc3_2012 generate group,COUNT(abc2012.$0);
group_2013 = foreach abc3_2013 generate group,COUNT(abc2013.$0);
group_2014 = foreach abc3_2014 generate group,COUNT(abc2014.$0);
group_2015 = foreach abc3_2015 generate group,COUNT(abc2015.$0);
group_2016 = foreach abc3_2016 generate group,COUNT(abc2016.$0);



g_2011 = order group_2011 by $1 DESC;
g_2012 = order group_2012 by $1 DESC;
g_2013 = order group_2013 by $1 DESC;
g_2014 = order group_2014 by $1 DESC;
g_2015 = order group_2015 by $1 DESC;
g_2016 = order group_2016 by $1 DESC;


ans_2011 = LIMIT g_2011 1;
ans_2012 = LIMIT g_2012 1;
ans_2013 = LIMIT g_2013 1;
ans_2014 = LIMIT g_2014 1;
ans_2015 = LIMIT g_2015 1;
ans_2016 = LIMIT g_2016 1;
--dump ans1;

h1b_ans = UNION ans_2011,ans_2012,ans_2013,ans_2014,ans_2015,ans_2016;
dump h1b_ans;
--year 2012
--abc1 = FILTER group2 BY $0 == 'DATA ENGINEER';
--abc2 = FILTER abc1 BY $2 == '2012';
--abc3 = group abc2 by ($0,$1,$2);
--dump abc3;

--group4 = foreach abc3 generate group,COUNT(abc2.$0);
--group5 = order group4 by $1 DESC;
--ans2 = LIMIT group5 1;
--dump ans2;

--year 2013



