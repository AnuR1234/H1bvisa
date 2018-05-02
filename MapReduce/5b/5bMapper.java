package project_5b;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;	


public static class CertifiedJobMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] record = value.toString().split("\t");
			String year = record[7];
			String job_title = record[4];
			if(record[1].equals("CERTIFIED"))
			{
			context.write(new Text(job_title), new Text(year));
		}
		
	}
	}
