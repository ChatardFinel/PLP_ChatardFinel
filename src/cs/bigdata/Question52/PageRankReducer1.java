package cs.bigdata.Question52;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import cs.bigdata.Question52.Couple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;


public class PageRankReducer1 extends Reducer<Text, Couple, Text, Text> {

    
    @Override
    public void reduce(final Text key, final Iterable<Couple> values,
            final Context context) throws IOException, InterruptedException {

    		//initialisation des variables
    	 	String listePageTo="";
    	 	int nbrPageTo = 0;
    	 	Text txt=new Text();
        
    	 	//mise en place de l'iterateur sur les couples (pageTo, pageRankTo)
    	 	Iterator<Couple> iterator = values.iterator();
        int i=1;
        
        while (iterator.hasNext()) {
        		Couple couple = iterator.next();
        		//on ne met pas de # devant le premier couple
        		if (i==1) {
        			listePageTo=listePageTo+couple.toText();
        			i+=1;
        			nbrPageTo+=1;
        		}
        		//on separe les couples par des #
        		else {
        			listePageTo=listePageTo+"#"+couple.toText();
        			nbrPageTo+=1;
        		}
        }
        //set de la valeur : (nbrPageTo;Couple1#Couple2 ...)
        txt.set(nbrPageTo+";"+listePageTo);

  
        // context.write(key, new IntWritable(sum));
        context.write(key, txt);
    }
}