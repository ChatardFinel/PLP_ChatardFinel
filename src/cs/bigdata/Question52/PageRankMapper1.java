package cs.bigdata.Question52;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import cs.bigdata.Question52.Couple;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class PageRankMapper1 extends Mapper<LongWritable, Text, Text, Couple> {

	private final static Couple coupleFrom = new Couple();
	private final static Couple coupleTo = new Couple();
	double pagerank = 1.0/75879.0;
	
	// Overriding of the map method
	
	@Override
	protected void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
	{
		// To complete according to the processing
		// Processing : keyI = ..., valI = ...

		//set des couples clé :(pageFrom, pageRankFrom), values: (pageTo, pageRankTo)
		String pageFrom = values.toString().split("\t")[0];
		String pageTo = values.toString().split("\t")[1];
		
		coupleFrom.set(pageFrom, pagerank);
		coupleTo.set(pageTo, pagerank);
		
		context.write(coupleFrom.toText(),coupleTo);
		
	}

	
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		
		cleanup(context);
	}
}
