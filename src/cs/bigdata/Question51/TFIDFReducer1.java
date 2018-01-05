package cs.bigdata.Question51;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;


public class TFIDFReducer1 extends Reducer<MotDoc, IntWritable, Text, IntWritable> {

    private IntWritable NbrMots = new IntWritable();

    @Override
    public void reduce(final MotDoc key, final Iterable<IntWritable> values,
    		final Context context) throws IOException, InterruptedException {

        		int sum = 0;
        		Iterator<IntWritable> iterator = values.iterator();

        		while (iterator.hasNext()) {
        			sum += iterator.next().get();
        		}

        		NbrMots.set(sum);
        		//renvoie en sortie - clé:(mot,docID) valeur:NbrMots
        		context.write(key.toText(), NbrMots);
    }
}