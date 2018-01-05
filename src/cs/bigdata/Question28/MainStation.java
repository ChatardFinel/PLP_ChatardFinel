package cs.bigdata.Question28;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import cs.bigdata.Question27.Tree;

public class MainStation {

	public static void main(String[] args) throws IOException {
		
		//fichier source
		String localSrc = "/Users/Cassandre/eclipse-workspace/PLPYearHeightTree/sources/isd-history.txt";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		try{
			
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			//lecture ligne par ligne
			String line = br.readLine();
			
			//on ignore les 22 premieres lignes parce qu'elles ne contiennent pas de donnees
			for (int i=1;i<23;i+=1) {
				line = br.readLine();
			}
					
			while (line !=null){
			
				//traiter la ligne en cours
				System.out.println(Station.DisplayInfo(line));
				//aller a la ligne suivante
				line = br.readLine();
			}
		}
					
		finally{
			//fermer le fichier source
			in.close();
			fs.close();
		}

	}
}