package ordo;

import java.util.Scanner;

public class Trieur implements SortComparator {
	
	public Trieur() {
		
	}

	public boolean compare(String deb, String fin, String c) {
		
		boolean res = (c.compareTo(deb) >= 0) && (c.compareTo(fin) <= 0 );
		return res;
	}
	
	public boolean compare(int deb, int fin, int j) {

		return false;
	}

	public Object[][] proprieteTri(int nbReduce, SortType type) {
		Scanner sc = new Scanner(System.in);
		Object[][] res = new Object[nbReduce][2];

		/*for (int i=0; i<nbReduce; i++) {
			System.out.println("Début de tri : ");
			String str = sc.nextLine();

			if(type.equals(SortType.Mot)) {
				res[i][0] = str;
			} 
			else if(type.equals(SortType.Entier)) {
				res[i][0] = Integer.parseInt(str);
			}
			
			System.out.println("Fin de tri : ");
			String str2 = sc.nextLine();
			
			if(type.equals(SortType.Mot)) {
				res[i][1] = str2;
			} 
			else if(type.equals(SortType.Entier)) {
				res[i][1] = Integer.parseInt(str2);
			}
		}*/
		res[0][0] = "a";
		res[0][1] = "m";
		res[1][0] = "n";
		res[1][1] = "z";
		

		
		return res;
	}


	
	
	
	
	
}
