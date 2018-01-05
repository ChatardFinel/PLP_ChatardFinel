package cs.bigdata.Question51;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;


public class TFIDFReducer3 extends Reducer<Text, Text, Text, Text> {

	//initialisation des variables
	private MotDoc motETdoc=new MotDoc();
	private double tfidf;
	private double nbrDocs=2;
	private Text txt=new Text();

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {

		IntWritable docParMot=new IntWritable();
		int sum = 0;
		Iterator<Text> itr = values.iterator();

		//determination du nombre de docs ou le mot est present
		while (itr.hasNext()) {
			String itrStr=itr.next().toString();
			sum +=1;
			motETdoc.set(key,itrStr.split("#")[0]);
			
			if (itr.hasNext() && sum==1 || sum==2) {
				docParMot.set(2);
			}
			
			else {
				docParMot.set(1);
			}
			
			//calcul du tfidf
			double terme1=Double.parseDouble(itrStr.split("#")[2]) / Double.parseDouble(itrStr.split("#")[1]);
			double terme2=Math.log10(nbrDocs/docParMot.get());
			tfidf=terme1*terme2;
			
			
			txt.set(String.valueOf(tfidf));
			//on renvoie la sortie clé:(mot,doc) valeur:tfidf
			context.write(motETdoc.toText(), txt);
		}




	}
}