table1 = load '/home/hduser/Documents/H1bvisa/H1b_final' using PigStorage('\t') as (s_no,case_status,employer_name,soc_name,job_title,full_time_position ,prevailing_wage,year,worksite,longitute,latitute);
noheader = filter table1 by $0 > '0' ;
table2 = order noheader by $0;
table3 = group table2 by (year);
table4 = FOREACH table3 GENERATE FLATTEN(group) AS year,COUNT(table2.case_status) as total_case_status;

table5 = group table2 by (year,case_status);
--dump table5;
table6 = FOREACH table5 GENERATE FLATTEN(group) AS (year,case_status),COUNT(table2.case_status) as total_case_status;
join_table = join table6 by year, table4 by year;
table7 = foreach join_table generate $0,$1,$2,$4;

table8 = foreach table7 generate  $0,$1,$2,$3,CONCAT((chararray)ROUND_TO((float)(($2*100)/$3),2),'%');

--describe table8;
filtcer = filter table8 by ($1 matches 'CERTIFIED');
filtden = filter table8 by ($1 matches 'DENIED');
filtcerwith = filter table8 by ($1 matches 'CERTIFIED-WITHDRAWN');
filtwith = filter table8 by ($1 matches 'WITHDRAWN');
--dump filtyr2011;

union_all = union filtcer,filtden,filtcerwith,filtwith;

dump union_all;
--store filtcer into '/home/hduser/Documents/H1bvisaOP/Pig/certified';
--store filtden into '/home/hduser/Documents/H1bvisaOP/Pig/denied';
--store filtcerwith into '/home/hduser/Documents/H1bvisaOP/Pig/certified-withdrawn';
--store filtwith into '/home/hduser/Documents/H1bvisaOP/Pig/withdrawn';


--dump table8;

