use h1bvisa;

create table if not exists totalavg(job string,totalgrowthavgfor5years double)
row format delimited
fields terminated by '\t'
stored as textfile;

insert overwrite table totalavg select job,round(sum(totalavggrowth)/5,2) as totalgrowthavgfor5years from a2out group by job order by totalgrowthavgfor5years desc limit 5;

select * from totalavg;
