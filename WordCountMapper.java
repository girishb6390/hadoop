package com.acadgild.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map(LongWritable kays,Text values, Context context) throws IOException, InterruptedException
	{
		String data = values.toString();
		for(String key : data.split("\\W"))
		{
			context.write(new Text(key), new IntWritable(1) );
		}
	}
}

