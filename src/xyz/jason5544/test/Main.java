package xyz.jason5544.test;

import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	
	public static void main(String[] args)
	{
		
		String input = "/phone/input";
		String ouput = "/phone/output";
		
		Configuration conf = new Configuration();
		conf.set("date", "2017-05-17");//设置指定的日期
	    conf.set("timepoint", "09-17-24");//设置指定的时间
		try
		{
			Job job = new Job(conf);
			job.setJarByClass(Main.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(ouput));
			System.exit(job.waitForCompletion(true)? 0 : 1);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	
	}
			
	

}
