package ordo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import formats.*;
import formats.Format.Type;
import map.MapReduce;
import hdfs.*;

public class Job implements JobInterface{

	private int NumberOfReduces; 
	private int NumberOfMaps;
	private Type InputFormat;
	private Type OutputFormat;
	private String InputFname;
	private String OutputFname;
	private SortComparator SortComparator;
	private SortType sortType;
	
	// nombre de serveur daemons
	public static int nbDaemons = 3;
	

	public void startJob(MapReduce mr) {

		// définition du nombre de reduce que l'on va lancer
		this.setNumberOfReduces(2);
		this.setSortType(SortType.Mot);

		Thread t[] = new Thread[nbDaemons]; // tableau de threads pour lancer les map
		Thread tReduce[] = new Thread[NumberOfReduces]; // tableau de threads pour lancer les reduce

		// Création de l'objet CallBack 
		CallBack cb = null;
		try {
			cb = new CallBackImpl();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		/* Récupère les fragments et lance les Map */
		//Etablissement de la connexion avec le nameNode 
		NameNode nameNode = null;
		try {	
			String URL = "//" + NameNodeImpl.machine + ":"+ NameNodeImpl.Port+ "/NameNode";
			System.out.println(URL);
			nameNode = (NameNode)Naming.lookup(URL);

		} catch(Exception e) {
			e.printStackTrace();
		}
	
		
		// Récupération de la liste des fragments associée 
		HashMap<String, String> listeMachineFrag = null;
		try {
			listeMachineFrag = nameNode.localiserFichier(this.InputFname);
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		
		// Liste pour enregistrer les machines où les maps s'exécutent
		ArrayList<String> hosts = new ArrayList<String>();
	    // Liste pour enregistrer les ports utilisés
		ArrayList<Integer>ports = new ArrayList<Integer>();
		
		for (int i=0; i<nbDaemons; i++ ) {
			//Récupération du nom du fragment
			String fragName = this.InputFname+i;
			
			// récupération du nom de machine à partir de la hashmap
			
			String machineName = listeMachineFrag.get(fragName);
			System.out.println(machineName);
			String[] s = machineName.split("/");
			String host = s[0];
			hosts.add(host);
			int port = Integer.parseInt(s[1]);
			ports.add(port);
			
			// EXECUTION DES MAPS
			t[i] = new Slave(port+10, host, this, mr, fragName , cb);
			
			//Lancer le thread 
			t[i].start();
		}

		for (int i=0; i<nbDaemons; i++ ) {
			try {
				t[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// attente des CallBacks
		for (int i=0; i<nbDaemons; i++) {
			try {
				((CallBackImpl) cb).decrement();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
		// Création de l'objet CallBack 
		CallBack cb2 = null;
		try {
			cb2 = new CallBackImpl();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		/* lancement des sorts et des shuffles */
		for(int i=0; i<nbDaemons; i++) {
			// nom du fichier résultat du Map i
			String fragName1 = this.InputFname + i + "-res";
			System.out.println(fragName1);
			
			t[i] = new SlaveShuffle(ports.get(i),NumberOfReduces, hosts.get(i),this.InputFname, this, fragName1,i,cb2,sortType);
			t[i].start();
		}
		System.out.println("av callback2");
		// attente des CallBacks
		for (int i=0; i<nbDaemons; i++) {
			try {
				((CallBackImpl) cb2).decrement();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} 
	
		try {
			nameNode.ajouterFich(this.InputFname, HdfsClient.PART_SIZE, NumberOfReduces);

		} catch (RemoteException e) {
			
			e.printStackTrace();
		}
		// Création de l'objet CallBack 
		CallBack cb3 = null;
		try {
			cb3 = new CallBackImpl();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}	
		ArrayList<String> ListeReduces = new ArrayList<String>();

		String fichier ="config/listeReduces.txt";
		//lecture du fichier listeMachines	
		try{
			InputStream ips = new FileInputStream(fichier); 
			InputStreamReader ipsr = new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String ligne;
			while ((ligne = br.readLine())!=null){
				ListeReduces.add(ligne);
			}
			br.close(); 
		}		
		catch (Exception e){
			System.out.println(e.toString());
		}
		
		
		// EXECUTION DES REDUCES
		for(int i=0; i < NumberOfReduces; i++) {
			String[] s = ListeReduces.get(i).split("/");
			String host = s[0];
			int port = Integer.parseInt(s[1]);
			
			String fragName = this.InputFname +"R";
			tReduce[i] = new SlaveBis(port+10,fragName,i,host, this,mr, cb3,nameNode);
			tReduce[i].start();
			
		} 
		for (int i=0; i < NumberOfReduces; i++ ) {
			try {
				tReduce[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} 

		// attente des CallBacks
		for (int i=0; i<NumberOfReduces; i++) {
			try {
				((CallBackImpl) cb3).decrement();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		try {
			nameNode.ajouterFichRed(this.InputFname, NumberOfReduces);
		} catch (RemoteException e) {
			e.printStackTrace();
		}	
		HdfsClient.HdfsRead(this.InputFname+"R", this.InputFname + "res",NumberOfReduces);
		
		} 
	

	
	
	public SortType getSortType() {
		return sortType;
	}

	public void setSortType(SortType sortType) {
		this.sortType = sortType;
	}




	public void setNumberOfReduces(int tasks) {
		this.NumberOfReduces = tasks;
	}

	public void setNumberOfMaps(int tasks) {
		this.NumberOfMaps = tasks;
	}

	public void setInputFormat(Type ft) {
		this.InputFormat = ft;
	}

	public void setOutputFormat(Type ft) {
		this.OutputFormat = ft;
	}

	public void setInputFname(String fname) {
		this.InputFname = fname;
	}

	public void setOutputFname(String fname) {
		this.OutputFname = fname;
	}

	public void setSortComparator(SortComparator sc) {
		this.SortComparator = sc;
	}

	public int getNumberOfReduces() {
		return this.NumberOfReduces;
	}

	public int getNumberOfMaps() {
		return this.NumberOfMaps;
	}

	public Type getInputFormat() {
		return this.InputFormat;
	}

	public Type getOutputFormat() {
		return this.OutputFormat;
	}

	public String getInputFname() {
		return this.InputFname;
	}

	public String getOutputFname() {
		return this.OutputFname;
	}

	public SortComparator getSortComparator() {
		return this.SortComparator;
	}
}