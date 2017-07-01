package task10;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	public void map(LongWritable key,Text value, Context context)throws IOException , InterruptedException{
		
		String line = value.toString().replace("|", ",");                         //convert the value into string 			
		String[] arr= line.split(",");						// split operation carried out 
		String company = arr[0]; 		// putting company name into new string 
		String state = arr[3];        // state name;
	
		String join1 = company+"\t"+state ;    
		
		context.write(new Text(join1), new IntWritable(1));    
		
		
	}

}
