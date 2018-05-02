package com.h1bvisa.qns3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public static class JobScientistMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Text key1  = new Text();
		private final static IntWritable one = new IntWritable(1);
		protected void map(LongWritable key,Text value, Context context)throws IOException, InterruptedException {
			
			
			String[] record = value.toString().split("\t");
			
			String industry = record[3];
			
			String job_title = record[4];
			
			String case_status = record[1];
			
			if(job_title.contains("DATA SCIENTIST") && case_status.equals("CERTIFIED"))
			{
				key1.set(industry);
		
				context.write(key1,one);
			}
			
			
			
		}
	}
