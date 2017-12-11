package xyz.jason5544.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

class LineException extends Exception
{
	private static final long serialVersionUID = 8245008693589452584L;
	int flag;
	public LineException(String msg, int flag)
	{
		super(msg);
		this.flag = flag;
	}
	
	public int getFlag()
	{
		return flag;
	}
}


public class TableLine {
	
	private String imsi, position, time, timeFlag;
	private Date day;
	private SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public void set(String line, boolean source, String date, String[] timepoint) throws LineException
	{
		String[] lineSplit = line.split("\t");
		if (source)
		{
			this.imsi = lineSplit[0];
			this.position = lineSplit[3];
			this.time = lineSplit[4];
		}
		else
		{
			this.imsi = lineSplit[0];
			this.position = lineSplit[2];
			this.time = lineSplit[3];
		}
		
		if (!this.time.startsWith(date))
		{
			throw new LineException("",-1);
		}
		
		try
		{
			this.day = this.formater.parse(this.time);
		}
		catch (ParseException e)
		{
			throw new LineException("", 0);
		}
		
		int i = 0, n = timepoint.length;
		int hour = Integer.valueOf(this.time.split("\\s+")[1].split(":")[0]);
		while (i < n && Integer.valueOf(timepoint[i]) < hour)
			i++;
		
		if (i < n)
		{
			if (i == 0)
			{
				this.timeFlag = ("00-" + timepoint[i]);
			}
			else
			{
				this.timeFlag = (timepoint[i-1] + "-" + timepoint[i]);
			}
		}
		else
		{
			throw new LineException("", -1);
		}
	}
	
	public Text outKey()
	{
		return new Text(this.imsi + "|" + this.timeFlag);
	}
	
	public Text outValue()
	{
		long t = (day.getTime()/1000L);
		return new Text(this.position + "|" + String.valueOf(t));
	}

}












