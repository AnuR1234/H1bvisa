data = load '/home/hduser/Documents/H1bvisa/H1b_final' using PigStorage('\t') as (s_no: int,case_status: chararray, employer_name: chararray, soc_name: chararray, job_title: chararray, full_time_position: chararray,prevailing_wage: int,year: chararray, worksite: chararray, longitude: double, latitute: double);
data = foreach data generate $1,$2,$7;
data_2011 = filter data by ($2 matches '2011');
data_2012 = filter data by ($2 matches '2012');
data_2013 = filter data by ($2 matches '2013');
data_2014 = filter data by ($2 matches '2014');
data_2015 = filter data by ($2 matches '2015');
data_2016 = filter data by ($2 matches '2016');

groupdata2011onPetitionandEmp = group data_2011 by ($1,$2);
groupdata2012onPetitionandEmp = group data_2012 by ($1,$2);
groupdata2013onPetitionandEmp = group data_2013 by ($1,$2);
groupdata2014onPetitionandEmp = group data_2014 by ($1,$2);
groupdata2015onPetitionandEmp = group data_2015 by ($1,$2);
groupdata2016onPetitionandEmp = group data_2016 by ($1,$2);


count2011 = foreach groupdata2011onPetitionandEmp generate FLATTEN(group),COUNT(data_2011.$0);
count2012 = foreach groupdata2012onPetitionandEmp generate FLATTEN(group),COUNT(data_2012.$0);
count2013 = foreach groupdata2013onPetitionandEmp generate FLATTEN(group),COUNT(data_2013.$0);
count2014 = foreach groupdata2014onPetitionandEmp generate FLATTEN(group),COUNT(data_2014.$0);
count2015 = foreach groupdata2015onPetitionandEmp generate FLATTEN(group),COUNT(data_2015.$0);
count2016 = foreach groupdata2016onPetitionandEmp generate FLATTEN(group),COUNT(data_2016.$0);

top5Employers2011 = limit(order count2011 by $2 desc) 5;
top5Employers2012 = limit(order count2012 by $2 desc) 5;
top5Employers2013 = limit(order count2013 by $2 desc) 5;
top5Employers2014 = limit(order count2014 by $2 desc) 5;
top5Employers2015 = limit(order count2015 by $2 desc) 5;
top5Employers2016 = limit(order count2016 by $2 desc) 5;

union_all = union top5Employers2011,top5Employers2012,top5Employers2013,top5Employers2014,top5Employers2015,top5Employers2016;

uniondata = order union_all by $1;
dump uniondata;

store uniondata into '/home/hduser/Desktop/6.txt';

--store top5Employers2011 into '/home/hduser/Documents/H1bvisaOP/pig/4a.pig/top5Employers2011';
--store top5Employers2012 into '/home/hduser/Documents/H1bvisaOP/pig/4a.pig/top5Employers2012';
--store top5Employers2013 into '/home/hduser/Documents/H1bvisaOP/pig/4a.pig/top5Employers2013';
--store top5Employers2014 into '/home/hduser/Documents/H1bvisaOP/pig/4a.pig/top5Employers2014';
--store top5Employers2015 into '/home/hduser/Documents/H1bvisaOP/pig/4a.pig/top5Employers2015';
--store top5Employers2016 into '/home/hduser/Documents/H1bvisaOP/pig/4a.pig/top5Employers2016';

