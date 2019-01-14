package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import formats.Format.OpenMode;
import formats.FormatKVImpl;
import formats.KV;
import hdfs.NameNode;
import hdfs.NameNodeImpl;

public class ShuffleImpl extends UnicastRemoteObject  implements Shuffle {

	public ShuffleImpl() throws RemoteException {

	}


	public TreeMap<String, List<String>> sort(String filename) throws RemoteException {
		KV read;
		List<String> valeurs = new ArrayList<String>();

		// liste résultat du tri
		TreeMap<String,List<String>> listeTriee = new TreeMap<String,List<String>>();

		// ouverture du fichier résultat du map 
		FormatKVImpl KVfile = new FormatKVImpl(filename);
		KVfile.open(OpenMode.R);
	
		while((read = KVfile.read()) != null) {

			// si la liste contient la clé : on ajoute la valeur à la liste des valeurs
			if(listeTriee.containsKey(read.k)) {
				listeTriee.get(read.k).add(read.v);

			} else {

				// on crée la liste des valeurs
				List<String> val = new ArrayList<String>();
				// on y ajoute la valeur v
				val.add(read.v);
				listeTriee.put(read.k,val);

			}
		}

		return listeTriee;
	}

	public void shuffle(TreeMap<String, List<String>> liste, String fileName, int nbReduce, SortType type, int id,String machineName, CallBack cb) throws RemoteException  {
		SortComparator comparateur = new Trieur();
		// récupération de la propriété de fin de tri
		Object[][] res = comparateur.proprieteTri(nbReduce,type);
		FormatKVImpl[] filetab = new FormatKVImpl[nbReduce];
		//Etablissement de la connexion avec le nameNode 
		NameNode nameNode = null;
		try {	
			String URL = "//" + NameNodeImpl.machine + ":"+ NameNodeImpl.Port+ "/NameNode";
			System.out.println(URL);
			nameNode = (NameNode)Naming.lookup(URL);

		} catch(Exception e) {
			e.printStackTrace();
		}


		// on crée un fichier pour chaque reduce que l'on ouvre en écriture
		for(int j=0; j<nbReduce; j++) {
			String fname = fileName + "R"+j+id;
			filetab[j] = new FormatKVImpl(fname);
			nameNode.ajouterFrag( machineName, fname);
			filetab[j].open(OpenMode.W);
		} 


		boolean triee = false;
		int i = 0;
		for(String k : liste.keySet()) {
			while(!triee && i < nbReduce) {
				if(comparateur.compare((String) res[i][0],(String) res[i][1], Character.toString(k.charAt(0)).toLowerCase())) {
					int s=0;
					for(String j:liste.get(k)){
						int n = Integer.parseInt(j);
						s = s+n;
					}
					String nbKey = Integer.toString(s);
					KV kv = new KV(k,nbKey);
					System.out.println(k + " : " + nbKey);
					filetab[i].write(kv);
					triee = true;
				}
				i++;
			}
			i = 0;
			triee = false;
		}
		
		// fermeture des fichiers
		for(int j=0; j<nbReduce; j++) {
			filetab[j].close();
		}
		
		cb.increment();
	}
	
	public static void main(String args[]) {
		int port;
		try {
			Integer I = new Integer(args[0]);
			port = I.intValue();
		} catch (Exception ex) {
			System.out.println("Please enter: java DaemonImpl <port>");
			return;
		}
		try {
			
			//On crée une instance de l'objet serveur 
			Shuffle shuffle = new ShuffleImpl();
			
			//Enregistrement de l'objet crée auprès du registry 
			LocateRegistry.createRegistry(port);
			String adresse = InetAddress.getLocalHost().getHostName();
			String URL = "//" + adresse + ":" + args[0] + "/mon_Shuffle";
			System.out.println(URL);
			Naming.rebind(URL,shuffle);
			System.out.println("Shuffle bound in registry");
		
		} catch (Exception exc) {
			exc.printStackTrace();
		}

	}
}
