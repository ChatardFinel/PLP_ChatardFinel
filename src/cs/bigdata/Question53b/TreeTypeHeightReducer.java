package cs.bigdata.Question53b;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;


public class TreeTypeHeightReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	private FloatWritable maxTypeHeight = new FloatWritable();

	@Override
	public void reduce(final Text key, final Iterable<FloatWritable> values,
	    final Context context) throws IOException, InterruptedException {

	        float maximum = 0;
	        Iterator<FloatWritable> iterator = values.iterator();

	        while (iterator.hasNext()) {
	            maximum = Math.max(iterator.next().get(), maximum);
	        }

	        maxTypeHeight.set(maximum);
	        // context.write(key, new IntWritable(sum));
	        context.write(key, maxTypeHeight);
	}
}
