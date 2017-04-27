package Query7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q7mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	String[] parts = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	String year = parts[7];
	int count =1;
	context.write(new Text(year), new IntWritable(count));
	
	}
}