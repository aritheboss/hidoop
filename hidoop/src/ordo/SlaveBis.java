package ordo;

import java.rmi.Naming;

import formats.Format;
import formats.FormatKVImpl;
import hdfs.HdfsClient;
import hdfs.NameNode;
import map.MapReduce;

public class SlaveBis extends Thread {
	

	private int port;
	private int id;
	private String fileName;
	private String host;
	private Job j;
	private MapReduce mr;
	private CallBack cb;
	private NameNode nameNode;
	
	

	public SlaveBis(int port,String fileName, int id, String host, Job j, MapReduce mr, CallBack cb, NameNode n) {
		this.port = port;
		this.host = host;
		this.fileName = fileName;
		this.id = id;
		this.j = j;
		this.mr = mr;
		this.cb = cb;
		this.nameNode = n;
	}

	public void run() {
		//Recuperation d'un stub sur l'objet serveur pour executer la methode runReduce
		
		try {
			
			String URL = "//" + this.host + ":" + this.port + "/mon_Daemon";
			System.out.println(URL);
			Daemon monServeur = (Daemon)Naming.lookup(URL);
			String readerName = "ConcatRes.txt"+this.id;
			
			Format reader = new FormatKVImpl(readerName);
		
			HdfsClient.HdfsRead(this.fileName+this.id, readerName,HdfsClient.PART_SIZE);
			
			String writerName = this.fileName+this.id;

			Format writer = new FormatKVImpl(writerName);
			
			//Execution du reduce
			monServeur.runReduce(mr, reader, writer, cb);
			this.port=this.port-10;
			
			this.nameNode.ajouterFrag(this.host + "/" + this.port,writerName);
			
		
		} catch(Exception e) {
			e.printStackTrace();
			}
		}
}
