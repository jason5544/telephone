package xyz.jason5544.test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import xyz.jason5544.test.BaseStationDataPreprocess.Counter;

public class Reduce extends Reducer<Text, Text, NullWritable, Text>{
	private String date;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void setup(Context context)
	{
		this.date = context.getConfiguration().get("date");
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String imsi = key.toString().split("\\|")[0];
		String timeFlag = key.toString().split("\\|")[1];
		
		TreeMap<Long, String> uploads = new TreeMap<Long, String>();
		String valueString;
		
		for (Text val:values)
		{
			valueString = val.toString();
			try
			{
				uploads.put(Long.valueOf(valueString.split("\\|")[1]), valueString.split("\\|")[0]);
			}
			catch (NumberFormatException e)
			{
				context.getCounter(Counter.TIMESKIP).increment(1);
				continue;
			}
		}
		
		try
		{
			Date tmp = this.formatter.parse(this.date + " " + timeFlag.split("-")[1] + ":00:00");
			uploads.put(tmp.getTime()/1000L, "OFF");
			HashMap<String, Float> locs = getStayTime(uploads);
			
			for (Entry<String, Float> entry:locs.entrySet())
			{
				StringBuilder builder = new StringBuilder();
				builder.append(imsi).append("|");
				builder.append(entry.getKey()).append("|");
				builder.append(timeFlag).append("|");
				builder.append(entry.getValue());
				context.write(NullWritable.get(), new Text(builder.toString()));
			}
		}
		catch (Exception e)
		{
			context.getCounter(Counter.USERSKIP).increment(1);
			return;
		}
	}
	
	private HashMap<String, Float> getStayTime(TreeMap<Long, String> uploads)
	{
		Entry<Long, String> upload, nextUpload;
		HashMap<String, Float> locs = new HashMap<String, Float>();
		Iterator<Entry<Long, String>> it = uploads.entrySet().iterator();
		
		upload = it.next();
		
		while (it.hasNext())
		{
			nextUpload =it.next();
			float diff  = (float)(nextUpload.getKey()-upload.getKey())/60.0f;

			if (diff < 60.0)
			{
				if (locs.containsKey(upload.getValue()))
				{
					locs.put(upload.getValue(), locs.get(upload.getValue()) + diff);
				}
				else
				{
					locs.put(upload.getValue(), diff);
				}
			}
			upload = nextUpload;
		}
		
		return locs;
	}
}












