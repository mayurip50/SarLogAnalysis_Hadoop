package com.sarlog;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class sarLogMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
private FloatWritable perseVal=new FloatWritable();
private Text keys=new Text();
	
public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
	
String[] valueTokens=value.toString().split(" ");
String Hostname=valueTokens[0];
String date="";
String timestamp="";
for(int i=1;i<valueTokens.length;i++) {
if(valueTokens[i].length()>0) {
	timestamp=valueTokens[i];
	break;
}
}
try {
date=timestamp.split(",")[0];
}
catch(Exception e) {
e.printStackTrace();	
}
float cpuper=100.0f - Float.parseFloat(valueTokens[valueTokens.length-1]);
perseVal.set(cpuper);
keys.set(Hostname+"\t"+date);

context.write(keys, perseVal);
}
}
