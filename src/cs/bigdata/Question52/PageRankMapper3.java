package cs.bigdata.Question52;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PageRankMapper3 extends Mapper<LongWritable, Text, Text, Text> {
	

	// Overriding of the map method
	@Override
	protected void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
	{
		// To complete according to the processing
		// Processing : keyI = ..., valI = ...
		Text newKey=new Text(values.toString().split("\t")[0]);
		Text newVal=new Text(values.toString().split("\t")[1]);
		context.write(newKey, newVal);
	}


	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}
}
