package cs.bigdata.Question53a;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class TreeTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private Text type = new Text();
// Overriding of the map method
@Override
	protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException{
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
	
		//Le type est dans la 2eme colonne du fichier csv
		String line = valE.toString().split(";")[2];
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens()){
			type.set(tokenizer.nextToken());
			context.write(type, one);
		}
    }

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}
