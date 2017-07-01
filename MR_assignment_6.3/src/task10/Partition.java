package task10;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<Text, IntWritable>{
	
	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions){
		
		String line = key.toString();
		String[] token = line.split("\t");
		
		
		if(token[0].equalsIgnoreCase("Akai") ){
			//System.out.println("reducer 1 ");
			return 0;
		}
		else if (token[0].equalsIgnoreCase("Lava") ){
			return 1  ;
				
		}
		else if(token[0].equalsIgnoreCase("Onida") ) {
			return 2  ;
		}
		else if (token[0].equalsIgnoreCase("Samsung") ){
			return 3  ;
		}
		else if(token[0].equalsIgnoreCase("Zen")){
			return 4 ;
		}
		else {
			
			return 5 ;
		}
			
		
	}

}
