package project_5b;
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public static class CertifiedJobReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		TreeMap<Integer, Text> map = new TreeMap<Integer, Text>();

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException 
		{
			int count = 0;
			String year = "";
			for(Text val : values)
			{
				 year = val.toString();
				 count++;
			}
			
			String Job_title = key.toString();
			String myValue = year+","+Job_title +","+count;
			
			map.put(new Integer(count), new Text(myValue));
			if(map.size() > 10)
			{
				map.remove(map.firstKey());
			}
		}

		@Override
		protected void cleanup( Context context)throws IOException, InterruptedException 
		{
			for(Text top10 : map.descendingMap().values())
			{
				context.write(NullWritable.get(), top10);
			}
		}
		
		
		
	}
