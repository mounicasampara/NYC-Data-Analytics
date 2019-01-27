package Task2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.IterationEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver
{
	
	public static void main(String[] args) throws Exception
	{
		
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Task-2");
		job.setJarByClass(Driver.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


class MapperClass extends Mapper <LongWritable, Text, Text , IntWritable>
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
		String token=value.toString();
		String[] tokens=token.split(",");
		
		if(tokens[0]!="#DATE") {
			
		
			String s1 = "m1_" + tokens[0];
			
			context.write(new Text(s1.toString()), new IntWritable(1));
			
			
			String s2 = "m2_" + tokens[1];
			int v2 =  Integer.parseInt(tokens[4])+ Integer.parseInt(tokens[6])+ Integer.parseInt(tokens[8])+ Integer.parseInt(tokens[10]);

			context.write(new Text(s2), new IntWritable(v2));
			

			String s3 = "m3_" + tokens[2];
			int v3 = 	Integer.parseInt(tokens[4])+ Integer.parseInt(tokens[6])+ Integer.parseInt(tokens[8])+ Integer.parseInt(tokens[10]);
			context.write(new Text(s3), new IntWritable(v3));
			
			
			String s4 = "m4_" + tokens[11];
			
			context.write(new Text(s4), new IntWritable(1));
			
			String[] year = tokens[0].split("/");
			

			String s5 = "m5_" + year[2];
			int v5 = 	  Integer.parseInt(tokens[3]) + Integer.parseInt(tokens[5]);
			context.write(new Text(s5), new IntWritable(v5));
			
			
			String s6 = "m6_" + year[2];
			int v6 = Integer.parseInt(tokens[4])+ Integer.parseInt(tokens[6]);
			context.write(new Text(s6), new IntWritable(v6));
			
			
			String s7 = "m7_" + year[2];
			int v7 = Integer.parseInt(tokens[7])+ Integer.parseInt(tokens[8]);
			context.write(new Text(s7), new IntWritable(v7));
			
			
			String s8 = "m8_" + year[2];
			int v8 = Integer.parseInt(tokens[9]) + Integer.parseInt(tokens[10]);
			context.write(new Text(s8), new IntWritable(v8));
			

		}
	
	}
		
}


class ReducerClass extends Reducer <Text, IntWritable, Text, IntWritable>
{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		
		IntWritable intWritable1   = new IntWritable();
		Text text1 = new Text();
		int sumvalue = 0;
		
		for (IntWritable val : values)
		{
			sumvalue += val.get();	
		}
		text1.set(key);
		intWritable1.set(sumvalue);
		context.write(text1,intWritable1);
	 		
	}
}