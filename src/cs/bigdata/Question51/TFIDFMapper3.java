package cs.bigdata.Question51;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

// To complete according to your problem
public class TFIDFMapper3 extends Mapper<LongWritable,Text, Text, Text> {
	
	
	private final static Text sortie = new Text();
	private String sortieStr = "";
	private Text mot = new Text();
// Overriding of the map method
	
@Override
protected void map(LongWritable key, Text values, Context context) throws IOException,InterruptedException
    {
        // To complete according to the processing
        // Processing : keyI = ..., valI = ...
		
		//recuperation des donnees en entree
		String motETdoc = values.toString().split("\t")[0];
		String motsParDocETnbrMots=values.toString().split("\t")[1];
		
		mot.set(motETdoc.split(";")[0]);
		sortieStr=motETdoc.split(";")[1]+"#"+motsParDocETnbrMots.split(";")[1]+"#"+motsParDocETnbrMots.split(";")[0];
		sortie.set(sortieStr);
		
		//renvoie la sortie - clé:mot valeur:docID#nbrMot#motsParDoc
		context.write(mot, sortie);
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}