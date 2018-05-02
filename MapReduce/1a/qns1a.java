


import java.io.IOException;
//import java.util.Iterator;
//import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataEngineerJob {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text myKey = new Text();
		IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			
			String job_title = record[4];
			
			String year = record[7];
			
			if(job_title.contains("DATA ENGINEER") && job_title != null)
			{
				myKey.set(year);
				
				context.write(myKey, one);
			}
		}
		
	}
	public static class Myreducer extends Reducer <Text,IntWritable,Text,LongWritable> 
	{ 
	private Text year = new Text();
	private Text avgpercent = new Text();
	int i=0; 
    String[] years={"2011","2012","2013","2014","2015","2016"}; 
    long [] a=new long[6]; 
    public void reduce(Text key,Iterable<IntWritable> values ,Context context) throws IOException, InterruptedException
    { 
    	long sum=0; 
    	for(IntWritable val:values) 
    		sum+=val.get(); 
    	a[i++]=sum; 
    } 

public void cleanup(Context context) throws IOException, InterruptedException 
{ 
for (int i=0;i<6;i++)     
if (i==0) 
context.write(new Text(years[i]), new Text("0")); 
else 
{
avg=((a[i]-a[i-1])*100/a[i-1]);
String avgper = avg + "%";
year.set(years[i]+"-"+year[i+1]);
avgpercent.set(avgper);
context.write(year,avgpercent); 

} 
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf,"Data Engineer Job Increasing");
		
		job.setJarByClass(DataEngineerJob.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}

}

