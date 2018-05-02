package com.h1bvisa.qns3;

import java.util.TreeMap;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public static class JobScientistReducer extends Reducer<Text, IntWritable, NullWritable, Text>
	{
		TreeMap<IntWritable,Text> desccountrofDataScientists = new TreeMap<IntWritable,Text>();
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException 
		{	
			
			int sum=0;
			for(IntWritable v :values)
			{
				sum+=v.get();
			}
			String key1 = key.toString();
			desccountrofDataScientists.put(new IntWritable(sum),new Text(key1));
			 
		}
			
			
		protected void cleanup(Context context) throws IOException, InterruptedException {
		        for (Text t: desccountrofDataScientists.descendingMap().values())
		            context.write(NullWritable.get(), t);

			
		
		}
		}
