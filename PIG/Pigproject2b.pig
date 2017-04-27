--find top 5 locations in the US who have got certified visa for each year


REGISTER /home/shubha/PROJECT/piggybank.jar;
DEFINE CSV_Storage org.apache.pig.piggybank.storage.CSVExcelStorage(); 
L1 = load '/home/shubha/PROJECT/h1b.csv' using CSV_Storage() as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);

g1 = foreach L1 generate $8, $1, $7;
g2 = filter g1 by $1 == 'CERTIFIED';



g3_2011 = filter g2 by $2 =='2011';
g3_2012 = filter g2 by $2 =='2012';
g3_2013 = filter g2 by $2 =='2013';
g3_2014 = filter g2 by $2 =='2014';
g3_2015 = filter g2 by $2 =='2015';
g3_2016 = filter g2 by $2 =='2016';


g4_2011 = group g3_2011 by ($0,$1,$2);
g4_2012 = group g3_2012 by ($0,$1,$2);
g4_2013 = group g3_2013 by ($0,$1,$2);
g4_2014 = group g3_2014 by ($0,$1,$2);
g4_2015 = group g3_2015 by ($0,$1,$2);
g4_2016 = group g3_2016 by ($0,$1,$2);

g5_2011 = foreach g4_2011 generate group, COUNT(g3_2011.$1);
g5_2012 = foreach g4_2012 generate group, COUNT(g3_2012.$1);
g5_2013 = foreach g4_2013 generate group, COUNT(g3_2013.$1);
g5_2014 = foreach g4_2014 generate group, COUNT(g3_2014.$1);
g5_2015 = foreach g4_2015 generate group, COUNT(g3_2015.$1);
g5_2016 = foreach g4_2016 generate group, COUNT(g3_2016.$1);


g_desc2011 = order g5_2011 by $1 desc;
g_desc2012 = order g5_2012 by $1 desc;
g_desc2013 = order g5_2013 by $1 desc;
g_desc2014 = order g5_2014 by $1 desc;
g_desc2015 = order g5_2015 by $1 desc;
g_desc2016 = order g5_2016 by $1 desc;

g_limit1 = limit g_desc2011 5;
g_limit2 = limit g_desc2012 5;
g_limit3 = limit g_desc2013 5;
g_limit4 = limit g_desc2014 5;
g_limit5 = limit g_desc2015 5;
g_limit6 = limit g_desc2016 5;

g_ans = UNION g_limit1, g_limit2, g_limit3, g_limit4, g_limit5, g_limit6;

dump g_ans;


