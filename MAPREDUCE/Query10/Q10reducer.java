package Query10;

import java.io.IOException;
//import java.util.TreeMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public  class Q10reducer extends Reducer<Text,Text,NullWritable,Text>{
	private TreeMap<Double, String> topten = new TreeMap<>();
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		 double total =0;
		 double successrate=0;
		 for (Text val:value)
		 {
			 String status = val.toString();
			 if(status.equals("\"CERTIFIED\"") || status.equals("\"CERTIFIED WITHDRAWN\""))
			 {
				total++ ;
			    successrate++;
			 }
			    else 
			    	total++;
		 }
		 
		double rate = (successrate/total)*100;
		
		
		if(rate >=70 && total >=1000){
		String op = key.toString()+ ","+ String.format("%.0f",total)+ "," +String.format("%f %%",rate);
		topten.put(rate, op);
		
		}
				 
	}
	protected void cleanup(Context context) throws IOException, InterruptedException{
		for(String val : topten.descendingMap().values()){
			context.write(NullWritable.get(),new Text(val));
		}
			
	
	}
}