/* une PROPOSITION, incomplète et adaptable... */

package ordo;

import java.util.HashMap;
import java.util.List;

public interface SortComparator {
	
	public Object[][] proprieteTri(int nbReduce, SortType type);
	public boolean compare(String deb, String fin, String c);
	public boolean compare(int deb, int fin, int j);
	
}