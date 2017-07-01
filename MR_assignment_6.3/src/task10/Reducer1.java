package task10;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	 HashMap<String, Integer> map = new HashMap<String, Integer>();
	 
	@Override 
	public void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
		
		int sum=0;
		for (IntWritable i : values){
			sum = sum + i.get() ;
		}
		
		String key1 = key.toString();
		map.put(key1,sum);
		
		//context.write(key, new IntWritable(sum));
	}
	
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		 
		 Object[] a = map.entrySet().toArray();
			Arrays.sort( a , new Comparator() {
			    public int compare(Object o1, Object o2) {
			        return ((Map.Entry<String, Integer>) o2).getValue()
			                   .compareTo(((Map.Entry<String, Integer>) o1).getValue());
			    }
			});
			
			int count = 0 ;
			for (Object e : a) {
				String key1 = ((Map.Entry<String, Integer>) e).getKey() ;
				int sum =  ((Map.Entry<String, Integer>) e).getValue(); 		
			
				if(count == 3) {break;}
				
				context.write(new Text(key1), new IntWritable(sum));
				  
			}
		 
		 
	 }
	 
	

}
