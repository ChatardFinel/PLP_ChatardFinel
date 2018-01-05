package cs.bigdata.Question52;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


public class PageRankReducer2 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException {

    	
        //initialisation des variables
        int nbrPageTo=0;
        double pageRankSuiv = 0;
        String str="";
        String listePageTo="";
        
        //mise en place de l'iterateur
        Iterator<Text> iterator = values.iterator();
       
        while (iterator.hasNext()) {
        		Text txtVal = iterator.next();
        		String str1 = txtVal.toString().split("/")[0];
        		String str2 = txtVal.toString().split(";")[0];
        		double pageRankTo = 0;
        		
        		
        		if (str1.equals(txtVal.toString())) {
        			if (str2.equals(txtVal.toString())) {
        				//dans le cas ou l'entree est de la forme (pageFrom: pageRank), on calcule la premiere partie du pageRank
        				//on somme avec les autres parties du pagerank
        				pageRankSuiv = pageRankSuiv + 0.15*Double.parseDouble(txtVal.toString()); 
        			}
        			else {
        				//dans le cas ou l'entree est de la forme (PageTo: PageTo1;PageTo2...)
        				listePageTo=listePageTo+txtVal.toString();
        				//on renvoie la meme chose : (PageTo: PageTo1;PageTo2...)
        				context.write(key, txtVal);
        			}
        		}
     
        		else{
        			//dans le cas ou l'entree est de la forme (pageTo: pageRank/nbrPage), on calcule la deuxieme partie du pageRank
        			pageRankTo = Double.parseDouble(str1);
    				nbrPageTo = Integer.parseInt( txtVal.toString().split("/")[1]);
    				//on somme avec les autres parties du pagerank
    				pageRankSuiv = pageRankSuiv + 0.85*(pageRankTo/nbrPageTo);
  
        		}
        }
        str=String.valueOf(pageRankSuiv);
        
  		 // context.write(key, new IntWritable(sum));
        // on renvoie le nouveau pagerank calcule
   		context.write(key, new Text(str) );
        
   		int i=1;
  
		while (i<listePageTo.split(";").length){
			String pageTo=listePageTo.split(";")[i];
			//on renvoie les pagerank recalcules associes a la pageTo de la page avec le nbrPageTo
			context.write(new Text(pageTo), new Text(str+"/"+String.valueOf(listePageTo.split(";").length-1)) );
			i=i+1;
   		}
    }  
		
		
}

  