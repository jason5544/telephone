package xyz.jason5544.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer.Context;

import xyz.jason5544.test.BaseStationDataPreprocess.Counter;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	String date;
	String[] timepoint;
	boolean dataSource;
	public void setup(Context context) throws IOException
	{
		
		System.out.println("--------------setup---------------------------");
		this.date = context.getConfiguration().get("date");
		System.out.println(context.getConfiguration().get("timepoint"));
		this.timepoint = context.getConfiguration().get("timepoint").split("-");
		FileSplit fs = (FileSplit)context.getInputSplit();
		String fileName = fs.getPath().getName();
		
		if (fileName.startsWith("pos"))
		{
			dataSource = true;
		}
		else if (fileName.startsWith("net"))
		{
			dataSource = false;
		}
		else
		{
			throw new IOException("file is not correct");
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		System.out.println("line: " + line);
		TableLine  tableLine = new TableLine();
		try
		{
			tableLine.set(line, this.dataSource, this.date, this.timepoint);
		}
		catch (LineException e)
		{
			if (e.getFlag() == -1)
			{
				context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
			}
			else 
			{
				context.getCounter(Counter.TIMESKIP).increment(1);
			}
			return;
		}
		catch (Exception e)
		{
			context.getCounter(Counter.LINESKIP).increment(1);
			return;
		}
		context.write(tableLine.outKey(), tableLine.outValue());
	}
}
