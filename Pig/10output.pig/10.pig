data = LOAD '/home/hduser/Documents/H1bvisa/H1b_final/' USING PigStorage('\t') as
(s_no:long,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:long,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);

temp= group data by $4;
total= foreach temp generate group,COUNT(data.$1);
--dump total;

--Count Total Applications who are 'CERTIFIED'

certified= filter data by $1 == 'CERTIFIED';
temp1= group certified by $4;
totalcertified= foreach temp1 generate group,COUNT(certified.$1);
--dump totalcertified;

--Count Total Applications who are 'CERTIFIED-WITHDRAWN'

certifiedWithdrawn= filter data by $1 == 'CERTIFIED-WITHDRAWN';
temp2= group certifiedWithdrawn by $4;
totalcertifiedwithdrawn= foreach temp2 generate group,COUNT(certifiedWithdrawn.$1);

--SUCCESS_RATE=(CERTIFIED+CERTIFIED-WITHDRAWN)/TOTAL X 100

joined= join totalcertified by $0,totalcertifiedwithdrawn by $0,total by $0;
joined= foreach joined generate $0,$1,$3,$5;
--dump joined;
intermediateoutput= foreach joined generate $0,(float)($1+$2)*100/($3),$3;
--dump intermediateoutput;
intermediateoutput2= filter intermediateoutput by $1>70 and $2>1000;	--Filter by success-rate greater than 70% and petition count above 1000
finaloutput= limit(order intermediateoutput2 by $1 DESC)10;
dump finaloutput;
--store finaloutput into '/home/hduser/Desktop/10.pig';

