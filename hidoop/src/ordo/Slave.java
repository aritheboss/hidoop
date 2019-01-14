package ordo;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import formats.Format;
import formats.FormatKVImpl;
import formats.FormatLINEImpl;
import map.MapReduce;

public class Slave extends Thread {
	
	private int port;
	private String host;
	private Job j;
	private MapReduce mr;
	private String readerName;
	private CallBack cb;
	
	
	public Slave(int port, String host, Job j, MapReduce mr, String readerName, CallBack cb) {
		this.port = port;
		this.host = host;
		this.j = j;
		this.mr = mr;
		this.readerName = readerName;
		this.cb = cb;
	}

	public void run() {
		//Recuperation d'un stub sur l'objet serveur pour executer la methode runMap
		
		try {
			
			
	    	String URL = "//" + this.host + ":" + this.port + "/mon_Daemon";
			System.out.println(URL);
			Daemon monServeur = (Daemon)Naming.lookup(URL);
			
			// Récupération du fichier qui contient le fragment à traiter
			Format reader = new FormatKVImpl(this.readerName);
			// Nom du fichier où stocker le resultat de map (à enlever si la modification de HdfsRead est faite)
			//String writerName = this.fileDestination + "/" + this.readerName + "-res";
			String writerName = this.readerName + "-res";
			//Creation du fichier correspondant
			Format writer = new FormatKVImpl(writerName);
						
			//Execution de map 
			monServeur.runMap(mr, reader, writer, cb);
		
		} catch(Exception e) {
			e.printStackTrace();
			}
		}
}
