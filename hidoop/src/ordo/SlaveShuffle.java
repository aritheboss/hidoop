package ordo;

import java.rmi.Naming;
import java.util.List;
import java.util.TreeMap;

import formats.Format;
import formats.FormatKVImpl;
import hdfs.HdfsClient;
import hdfs.NameNode;
import hdfs.NameNodeImpl;
import map.MapReduce;

public class SlaveShuffle extends Thread {
	private int port;
	private String host;
	private Job job;
	private MapReduce mr;
	private Shuffle shuffle;
	private String fileName;
	private int id;
	private int nbReduce;
	private String fname ; 
	private CallBack cb;
	SortType type;

	public SlaveShuffle(int p,int nb, String h, String fname,Job j, String f, int id, CallBack c, SortType t) {
		this.port = p;
		this. host = h;
		this.fname = fname;
		this.job = j;
		this.fileName = f;
		this.id = id;
		this.nbReduce = nb;
		this.cb = c;
		this.type = t;
	}
	

	@Override
	public void run() {
		try {	
			String URL = "//" + "solo" + ":" + "6666" + "/mon_Shuffle";
			System.out.println("shuffle : " + URL);
			Shuffle monShuffle = (Shuffle)Naming.lookup(URL);

			// Exécution du sort
			TreeMap<String, List<String>> listeTriee = monShuffle.sort(this.fileName);

			String machineName = this.host+"/"+this.port;
			// Exécution du shuffle
			monShuffle.shuffle(listeTriee, this.fname, nbReduce, type, id,machineName,cb);

		} catch(Exception e) {
			e.printStackTrace();
		}
	}


}
