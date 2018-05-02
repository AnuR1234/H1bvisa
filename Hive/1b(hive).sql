--fetching the petitions for each year for all job_titles
----------------------------------------------------------------------

use h1bvisa;

create table if not exists 1bavggrowth2011(year1 string,job string,p1 int)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table 1bavggrowth2011 select year,LOWER(job_title),count(*) as p from h1b_final where year = 2011 group by year,job_title;

create table if not exists 1bavggrowth2012(year2 string,job string,p2 int)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table 1bavggrowth2012 select year,LOWER(job_title),count(*) as p from h1b_final where  year = 2012 group by year,job_title;

create table if not exists 1bavggrowth2013(year3 string,job string,p3 int)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table 1bavggrowth2013 select year,LOWER(job_title),count(*) as p from h1b_final where year = 2013 group by year,job_title;

create table if not exists 1bavggrowth2014(year4 string,job string,p4 int)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table 1bavggrowth2014 select year,LOWER(job_title),count(*) as p from h1b_final where  year = 2014 group by year,job_title;

create table if not exists 1bavggrowth2015(year5 string,job string,p5 int)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table 1bavggrowth2015 select year,LOWER(job_title),count(*) as p from h1b_final where year = 2015 group by year,job_title;

create table if not exists 1bavggrowth2016(year6 string,job string,p6 int)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table 1bavggrowth2016 select year,LOWER(job_title),count(*) as p from h1b_final where year = 2016 group by year,job_title;

--annual growth for each year ((current_year-previous_year)*100/previous_year)
--------------------------------------------------------------

create table if not exists a2out(year string,job string,totalavggrowth double)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table a2out 

select * from(select a.year1,a.job,(sum(b.p2-a.p1)*100/sum(a.p1)) as avg1 from 1bavggrowth2011 a,1bavggrowth2012 b where a.job=b.job group by a.year1,a.job order by avg1 desc ) q1
union

select * from(select a.year2,a.job,(sum(b.p3-a.p2)*100/sum(a.p2)) avg1 from 1bavggrowth2012 a,1bavggrowth2013 b where a.job=b.job group by a.year2,a.job order by avg1 desc )q2

union
select * from(select a.year3,a.job,(sum(b.p4-a.p3)*100/sum(a.p3)) avg1 from 1bavggrowth2013 a,1bavggrowth2014 b where a.job=b.job group by a.year3,a.job order by avg1 desc  )q3

union
select * from(select a.year4,a.job,(sum(b.p5-a.p4)*100/sum(a.p4)) avg1 from 1bavggrowth2014 a,1bavggrowth2015 b where a.job=b.job group by a.year4,a.job order by avg1 desc  )q4

union
select * from(select a.year5,a.job,(sum(b.p6-a.p5)*100/sum(a.p5)) avg1 from 1bavggrowth2015 a,1bavggrowth2016 b where a.job=b.job group by a.year5,a.job order by avg1 desc   ) q5;


--adding all of them and finding the top 5 jobs 
-----------------------------------------------------------------------
create table if not exists totalavg(job string,totalgrowthavgfor5years double)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table totalavg select job,round(sum(totalavggrowth)/5,2) as totalgrowthavgfor5years from a2out group by job order by totalgrowthavgfor5years desc limit 5;

select * from totalavg;

--select job,totalavggrowth from a2out order by totalavggrowth desc limit 5;






