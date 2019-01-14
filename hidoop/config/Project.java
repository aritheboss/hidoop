package config;

public class Project {

	// constantes du projet
	public static final String PATH = "data/";
	
	// adresse ip du serveur (Name Node)
	public final static String ADRESSE_IP = InetAdress.getLocalHost();
	
	// port hdfs
	public final static int PORT_HDFS = 8081;
}
