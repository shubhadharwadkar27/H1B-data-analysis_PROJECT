package Query3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Q3driver {


public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration conf = new Configuration();
	  conf.set("mapreduce.output.textoutputformat.separator", ",");
	  Job job = Job.getInstance(conf, "Q3");
	  job.setJarByClass(Q3driver.class);
	  job.setMapperClass(Q3mapper.class);
	  job.setReducerClass(Q3reducer.class);
	  //job.setReducerClass(PositionReducer.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  job.setOutputKeyClass(NullWritable.class);
	  
	  job.setOutputValueClass(Text.class);
	  job.setInputFormatClass(TextInputFormat.class);
	  job.setOutputFormatClass(TextOutputFormat.class);
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}