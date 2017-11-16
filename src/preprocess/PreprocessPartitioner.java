package preprocess;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class PreprocessPartitioner implements Partitioner<Text, Text> {
	
	public void configure(JobConf job) {
	}

	public int getPartition(Text key, Text value, int numPartitions) {
		int lowerbound = 870393600;
		int upperbound = 1351727999;
		int interval = (upperbound - lowerbound)/numPartitions;
		
		for(int i = 0; i < numPartitions; i++){
			if(Integer.valueOf(key.toString()) <= lowerbound + interval*(i+1))
				return i;
		}
		return numPartitions-1;
	}
}
