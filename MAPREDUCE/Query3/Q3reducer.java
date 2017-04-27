package Query3;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q3reducer extends Reducer<Text, IntWritable, NullWritable, Text>{
	private TreeMap<Integer, Text> topind = new TreeMap<>();
	//private IntWritable result=new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
		int sum=0;
		
		for (IntWritable val:value){
			sum+=val.get();
		}
		String output = key.toString()+ ","+sum;
		topind.put(sum, new Text(output));
		if(topind.size()>1)
		{
			topind.remove(topind.firstKey());
		}
	}
		
	protected void	cleanup(Context context) throws IOException, InterruptedException{
			for(Text top :topind.values()){
				context.write(NullWritable.get(),top);
			}
	}
}