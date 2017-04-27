package Query10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



	public class Q10mapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String parts[] =value.toString().split(",");
			String status = parts[1];
			String jobposition =parts[4];
			context.write(new Text(jobposition), new Text(status));
		}
	}


