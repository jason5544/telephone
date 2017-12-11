package xyz.jason5544.test;

import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class Main {
	
	public static void main(String[] args)
	{
		
		String input = "/phone/input";
		String ouput = "/phone/output";
		
		Configuration conf = new Configuration();
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
