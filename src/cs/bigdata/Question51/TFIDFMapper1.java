package cs.bigdata.Question51;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class TFIDFMapper1 extends Mapper<LongWritable, Text, MotDoc, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private MotDoc motETdoc = new MotDoc();
	
// Overriding of the map method
@Override
protected void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException {
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
	
		String docIDext = ((FileSplit) context.getInputSplit()).getPath().toString().split("/")[7];
		String docID = docIDext.substring(0,docIDext.length()-4);
		
		//comme les textes sont en anglais il n'y a pas de caractères speciaux type lettres avec accent
		String ligne = values.toString().toLowerCase().replaceAll("\\W"," ") ;
		StringTokenizer tokenizer = new StringTokenizer(ligne);
		
		while(tokenizer.hasMoreTokens()){
			String mot = tokenizer.nextToken();
			motETdoc.set(new Text(mot),docID);
			//renvoie la sortie - clé:(mot;docID) valeur:1
			context.write(motETdoc, one);
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