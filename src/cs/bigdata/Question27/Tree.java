
package cs.bigdata.Question27;

public class Tree {
	public static String year;
	public static String height;
	
	public static String DisplayYearHeight(String line) {
		//fonction qui pour chaque arbre retourne un string de la forme "Annee - Hauteur"
		
		String str= "";
		
		// L'annee est un entier, sauf pour la premiere ligne
		// Pour certains arbres, l'annee n'est pas renseignee
		try {
			year= line.split(";")[5];
			Integer.parseInt(year);
			str= year+" - ";
		}
		//on renvoit quand meme la ligne avec N/A a la place de l'annee
		catch (NumberFormatException e) {
			str= "N/A - ";
		}
		
		// La hauteur est un nombre décimal, sauf pour la premiere ligne
		try {
			height= line.split(";")[6];
			Float.parseFloat(height);
			str= str+height;
		}
		//on renvoit quand meme la ligne avec N/A a la place de la hauteur
		catch (NumberFormatException e) {
			str= str+"N/A";
		}
		
		return str;
	}
}
