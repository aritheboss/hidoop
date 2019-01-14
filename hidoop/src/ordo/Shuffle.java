package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public interface Shuffle extends Remote {
	public TreeMap<String,List<String>> sort(String filename) throws RemoteException ;
	public void shuffle(TreeMap<String, List<String>> liste, String fileName, int nbReduce, SortType type, int id,String machineNAme,CallBack cb) throws RemoteException ;
	
}
