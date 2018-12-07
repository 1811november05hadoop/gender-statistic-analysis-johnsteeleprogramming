package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WordMapper;
import com.revature.reduce.MaxReducer;
import com.revature.reduce.SumReducer;

public class WordCount {

	public static void main(String[] args) throws Exception {
		
		if(args.length != 2) {
			System.out.println("Usage: WordCount <input dir> <output dir>");
			System.exit(-1);
		}
		
		// The MapReduce job object
		Job job = new Job();
		
		// The class that contains the main method.
		job.setJarByClass(WordCount.class);
		
		// Can name this whatever, but usually is named after the job, eg. 'Word Count'
		job.setJobName("Welcome to the Combiner");
		
		// paths will be similar to hdfs:\\
		// Set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Specify the mapper and reducer class
		job.setMapperClass(WordMapper.class);
		
		// The Combiner (Intermediate Reducer)
//		job.setReducerClass(SumReducer.class);
//		job.setCombinerClass(MaxReducer.class);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(MaxReducer.class);
		
		// Specify job final output key value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Run and check success
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);

	}
}