package preprocess;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Preprocess {
	public static void main(String[] args) throws Exception {
		Preprocess wc = new Preprocess();
		wc.run(args[0], args[1]); 
	}
	
	public void run(String inputPath, String outputPath) throws IOException {
		/* 
		 * You can lookup usage of these api from this website. =)
		 * http://tool.oschina.net/uploads/apidocs/hadoop/index.html?overview-summary.html
	   	 */
		
		JobConf conf = new JobConf(Preprocess.class);
		conf.setJobName("Preprocess");
		/*read one movie in 8 lines*/
		// conf.setInputFormat(NLineInputFormat.class);
        // NLineInputFormat.addInputPath(conf, new Path(inputPath));
        conf.set("textinputformat.record.delimiter", "\n\n");

		
		conf.setMapperClass(PreprocessMapper.class);
		conf.setReducerClass(PreprocessReducer.class);
		
		/*
		 * If your mapper output key-value pair is different from final 
		 * output key-value pair, please remember to setup these two APIs.
		 */ 
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		// TODO setup your key comparator class
		conf.setOutputKeyComparatorClass(PreprocessKeyComparator.class);
		// TODO setup your partitioner class
        conf.setPartitionerClass(PreprocessPartitioner.class);
		// TODO setup your key group comparator class
		//conf.setOutputValueGroupingComparator(WordCountGroupComparator.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setNumMapTasks(50);
		conf.setNumReduceTasks(10);

		JobClient.runJob(conf);
	}
}
