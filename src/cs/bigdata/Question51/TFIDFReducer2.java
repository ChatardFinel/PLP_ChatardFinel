package cs.bigdata.Question51;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;


public class TFIDFReducer2 extends Reducer<Text, MotNbrMots, Text, Text> {

	private NbrMots motsParDocETnbrMots = new NbrMots();
	private MotDoc motDoc=new MotDoc();

	@Override
	public void reduce(final Text key, final Iterable<MotNbrMots> values,
			final Context context) throws IOException, InterruptedException {
		
		//initialisation des variables
		int sum = 0;
		Text mot=new Text();
		
		//definition de l'iterateur
		Iterator<MotNbrMots> iterator = values.iterator();
		Collection<MotNbrMots> valuesCopy=new ArrayList<>();

		while (iterator.hasNext()) {
			MotNbrMots motNbrMots=iterator.next();
			sum += motNbrMots.getNbrMots().get();
			MotNbrMots copy =new MotNbrMots(motNbrMots.getMot(),motNbrMots.getNbrMots().get());
			valuesCopy.add(copy);
		}
		
		Iterator<MotNbrMots> itrCopy = valuesCopy.iterator();


		while (itrCopy.hasNext()) {
			MotNbrMots motNbrMots2=itrCopy.next();
			mot.set(motNbrMots2.getMot());
			motDoc.set(mot, key);
			motsParDocETnbrMots.set(motNbrMots2.getNbrMots(), sum);
			//renvoie la sortie clé:(mot,docID) valeur:(motParDoc;nbrMots)
			context.write(motDoc.toText(), motsParDocETnbrMots.toText());

		}




	}
}