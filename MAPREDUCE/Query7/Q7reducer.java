package Query7;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q7reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	   public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		   int no =0;
		   for(IntWritable val:value){
			   no+=val.get();
		   }
		   context.write(key, new IntWritable(no));
	   }
}