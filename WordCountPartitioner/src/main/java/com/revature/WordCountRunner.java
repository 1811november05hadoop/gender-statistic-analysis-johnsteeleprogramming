package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.AlphabetPartitioner;
import com.revature.map.WordMapper;
import com.revature.reduce.SumReducer;

public class WordCountRunner extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		if(args.length != 2) {
			System.err.println("Usage: WordCount <input dir> <output dir>");
			return -1;
		}
		
		// The MapReduce job object
		Job job = new Job();
		
		// The class that contains the main method.
		job.setJarByClass(WordCount.class);
		
		// Can name this whatever, but usually is named after the job, eg. 'Word Count'
		job.setJobName("Welcome to the MapReduce");
		
		// paths will be similar to hdfs:\\
		// Set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Specify the mapper class
		job.setMapperClass(WordMapper.class);
		
		//Specify the partitioner class
		job.setPartitionerClass(AlphabetPartitioner.class);
		
		// Set this up because AlphabetPartitioner will split data into 3 classes
		// For example, if splitting things up by month, would do 12.
		job.setNumReduceTasks(3);
		
		//Set Combiner (remove
//			job.setCombinerClass(SumReducer.class);
		
		//Specify the reducer class
		job.setReducerClass(SumReducer.class);
		
		// Specify job final output key value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Run and check success
		boolean success = job.waitForCompletion(true);
//		System.exit(success ? 0 : 1);
		if(success)
			return 0;
		else
			return 1;

	}
}