package cs.bigdata.Question51;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;


public class TFIDFMapper2 extends Mapper<LongWritable, Text, Text, MotNbrMots> {
	
	private final static MotNbrMots motNbrMots = new MotNbrMots();
	private Text docID = new Text();
	
// Overriding of the map method
@Override
protected void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
    {
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
	
		//recuperation des donnees en entree
		String motETdoc = values.toString().split("\t")[0];
		String NbrMots = values.toString().split("\t")[1];
		
		docID.set(motETdoc.split(";")[1]);
		motNbrMots.set(motETdoc.split(";")[0], Integer.parseInt(NbrMots));
		//renvoie en sortie - clé:docID  valeur:(mot, nbrMots)
		context.write(docID, motNbrMots);
		
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}