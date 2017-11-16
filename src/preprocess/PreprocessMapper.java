package preprocess;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PreprocessMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, 
		Reporter reporter) throws IOException{
	
			String lines = value.toString().replaceAll("\n", "");
//			System.out.println(key+lines);
			String[] attr = lines.split("review/");
			//System.out.println(Arrays.toString(attr));
			/*
			String productId = attr[0].substring(19);
			String userId = attr[1].substring(15);
			String score = attr[4].substring(14);
			String time = attr[5].substring(13);
			*/
			String productId = attr[0].split(": ")[1];
			String userId = attr[1].split(": ")[1];
			String score = attr[4].split(": ")[1];
			String time = attr[5].split(": ")[1];	
			String outputValue = productId + "_" + userId +"_"+ score;
			//System.out.println(outputValue);
			output.collect(new Text(time), new Text(outputValue));
	}
}