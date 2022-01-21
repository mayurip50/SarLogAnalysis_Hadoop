package com.sarlog;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class sarLogReducer extends Reducer<Text,FloatWritable,Text,Text>{

Text result=new Text();	
public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException {
float sum=0.0f;
int cnt=0;
for(FloatWritable val : value) {
sum+=val.get();
cnt++;
}
float agg=sum/cnt;
result.set(agg+"%");
context.write(key, result);
}	
}
