package ordo;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.net.InetAddress;
import java.rmi.*;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import map.Mapper;
import map.Reducer;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	private static final long serialVersionUID = 1L;


	public DaemonImpl() throws RemoteException {}

	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		reader.open(Format.OpenMode.R);
		writer.open(Format.OpenMode.W);
		
		//Exécution de programme map sur le fragment hébergé sur la machine
		m.map(reader, writer);
		writer.close();
		
		// Envoi du CallBack
		try {
			cb.increment();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void runReduce (Reducer r, Format reader, Format writer, CallBack cb) throws RemoteException {
		reader.open(Format.OpenMode.R);
		writer.open(Format.OpenMode.W);
		
		// Exécution du reduce sur les fragments résultats de la machine
		r.reduce(reader, writer);
		writer.close();
		
		// Envoi du CallBack
		try {
			cb.increment();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void main (String args[]) {
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
			Daemon obj = new DaemonImpl();
			
			//Enregistrement de l'objet crée auprès du registry 
			LocateRegistry.createRegistry(port);
			String adresse = InetAddress.getLocalHost().getHostName();
			String URL = "//" + adresse + ":" + args[0] + "/mon_Daemon";
			System.out.println(URL);
			Naming.rebind(URL,obj);
			System.out.println("DaemoImpl bound in registry");
		
		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}
}