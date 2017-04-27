package Query3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class Q3mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//String[] parts = value.toString().split(",");
		String [] parts=value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); 
		
		String employer_name = parts[2];
		String job_title = parts[4];
		int count =1;
		if(job_title.equals("\"DATA SCIENTIST\"")){
		context.write(new Text(employer_name),new IntWritable(count));
	}
}
}