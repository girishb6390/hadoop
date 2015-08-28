package com.acadgild.hadoop;

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class WordCountDriver {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJobName("WordCountJob");
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 1 : 0);
		
	}
}
