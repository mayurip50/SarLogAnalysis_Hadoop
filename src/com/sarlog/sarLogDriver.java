package com.sarlog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowActivityEntity;

public class sarLogDriver {

public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		
		String[] Otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
		if(Otherargs.length!=2) {
			System.err.println("Usage : MR Job<in> [in..]<out>");
			System.exit(2);
		}
		Job job=new Job(conf,"sarLog MR Job");
		job.setJarByClass(sarLogDriver.class);
		job.setMapperClass(sarLogMapper.class);
		//job.setCombinerClass(sarLogReducer.class);
		job.setReducerClass(sarLogReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		/*
		 * for(int i=0;i<Otherargs.length-1;i++) { FileInputFormat.addInputPath(job, new
		 * Path(Otherargs[i])); }
		 */	
		FileInputFormat.addInputPath(job,new Path(Otherargs[0]));
					
		FileOutputFormat.setOutputPath(job,new Path(Otherargs[1]));
			System.exit(job.waitForCompletion(true)?0:1);

	}

}
