
package cs.bigdata.Question28;

public class Station {
	public static String USAFcode;
	public static String name;
	public static String country;
	public static String elevation;
	
	public static String DisplayInfo(String line) {
		//fonction qui pour chaque station retourne un string de la forme "USAFCode:'xx' - name:'xxx' - country:'xx' - elevation:'xxx'"
		
		String str= "";
		
		
		// Le code USAF commence au 1er caractère de la ligne et contient 6 caracteres
		USAFcode=line.substring(1,6);
		// Le nom de la station commence au 13eme caractère de la ligne et contient 29 caracteres
		name=line.substring(13,42);
		// Le code pays commence au 43eme caractère de la ligne et contient 2 caracteres
		country=line.substring(43,45);
		// L'altitude commence au 74eme caractère de la ligne et contient 7 caracteres
		elevation=line.substring(74,81);
		str= "USAFCode: "+USAFcode+" - name: "+name+" - country: "+country+" - elevation: "+elevation;
		
		
		return str;
	}
}
