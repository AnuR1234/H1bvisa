
h1bvisa = load '/home/hduser/Documents/H1bvisa/H1b_final' using PigStorage('\t') AS (id:long,case_status:chararray,emp_name:chararray,soc_name:chararray,job_title:chararray,full_time_pos:chararray,prevailing_wage:long,year:chararray,worksite:chararray,lon:double,lat:double);

h1bvisa = foreach h1bvisa generate $4,$7,$8;

data_eng_2011 = filter h1bvisa by $0 matches '(.*)DATA ENGINEER(.*)' and $1 matches '2011';

group_worksite_2011 = group data_eng_2011 by $2;

count_job_2011 = foreach group_worksite_2011 generate '2011',group,COUNT($1);

order_job_2011 = order count_job_2011 by $2 desc;

limit_job_2011 = limit order_job_2011 1;



data_eng_2012 = filter h1bvisa by $0 matches '(.*)DATA ENGINEER(.*)' and $1 matches '2012';

group_worksite_2012 = group data_eng_2012 by $2;

count_job_2012 = foreach group_worksite_2012 generate '2012',group,COUNT($1);

order_job_2012 = order count_job_2012 by $2 desc;

limit_job_2012 = limit order_job_2012 1;



data_eng_2013 = filter h1bvisa by $0 matches '(.*)DATA ENGINEER(.*)' and $1 matches '2013';

group_worksite_2013 = group data_eng_2013 by $2;

count_job_2013 = foreach group_worksite_2013 generate '2013',group,COUNT($1);

order_job_2013 = order count_job_2013 by $2 desc;

limit_job_2013 = limit order_job_2013 1;




data_eng_2014 = filter h1bvisa by $0 matches '(.*)DATA ENGINEER(.*)' and $1 matches '2014';

group_worksite_2014 = group data_eng_2014 by $2;

count_job_2014 = foreach group_worksite_2014 generate '2014',group,COUNT($1);

order_job_2014 = order count_job_2014 by $2 desc;

limit_job_2014 = limit order_job_2014 1;




data_eng_2015 = filter h1bvisa by $0 matches '(.*)DATA ENGINEER(.*)' and $1 matches '2015';

group_worksite_2015 = group data_eng_2015 by $2;

count_job_2015 = foreach group_worksite_2015 generate '2015',group,COUNT($1);

order_job_2015 = order count_job_2015 by $2 desc;

limit_job_2015 = limit order_job_2015 1;

--dump limit_job_2015;


data_eng_2016 = filter h1bvisa by $0 matches '(.*)DATA ENGINEER(.*)' and $1 matches '2016';

group_worksite_2016 = group data_eng_2016 by $2;

count_job_2016 = foreach group_worksite_2016 generate '2016',group,COUNT($1);

order_job_2016 = order count_job_2016 by $2 desc;

limit_job_2016 = limit order_job_2016 1;

union_all = union limit_job_2011,limit_job_2012,limit_job_2013,limit_job_2014,limit_job_2015,limit_job_2016;

--dump union_all;
order_union = order union_all by $2 desc;

dump order_union;

--store order_union into '/home/hduser/Documents/H1bvisaOP/pig/2a.pig';



