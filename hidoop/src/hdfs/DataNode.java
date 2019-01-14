package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.Format.OpenMode;
import formats.LineFormat;

public class DataNode {

	
	// Liste des attributs de fragments
	private List<Fragment> fragments; 

	// 
	private DataNode() {
		fragments = new ArrayList<>();
	}
	
	
	private void launch(String[] args) {
		//On connecte ce dataNode au NameNode
		try {
			InetAddress adresseIp = InetAddress.getLocalHost();
			Socket hdfsServer = connectionAuServeurHDFS(args[1], Integer.parseInt(args[0]));
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		// serveurHDFS
		ServerSocket socketHDFS;

		try {
			// Ecoute sur le port entré en argument
			socketHDFS = new ServerSocket(Integer.parseInt(args[0]));

			// Lancement des thread
			while (true) {
				Thread t = new Thread(new TraitementGeneral(socketHDFS.accept()));
				t.start();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	
	/**
	 * Connexion entre le client DataNode et le NameNode
	 * @param 
	 * @return La socket qui permet d'accéder au serveur HDFS
	 */
	private static Socket connectionAuServeurHDFS(String adresseIp, Integer port) {

		// socket entre DataNode et NameNode
		Socket socketServeurHDFS = null;

		try {
			// socket entre HDFS et NameNode
			socketServeurHDFS = new Socket(Project.ADRESSE_IP, Project.PORT_HDFS);
			
			// Outil d'écriture sur la socket
			ObjectOutputStream oos = new ObjectOutputStream(socketServeurHDFS.getOutputStream());
			
			// C'est un chunk (et pas HDFS)
			oos.writeInt(1);
			
			// Ecriture de l'adresse et du numero de port
			oos.writeObject(adresseIp);
			oos.writeObject(port);

		} catch (IOException e) {
			throw new IllegalStateException("ServerHDFS impossible d'accès");
		}

		return socketServeurHDFS;
	}
	
	public static void main(String[] args) {
		new DataNode().launch(args);

    }
	
	
	/**
	 * Traiter la demande d'un client
	 */
	private class TraitementGeneral implements Runnable {
		/** Outils d'écriture et lecture sur la socket */
		private ObjectOutputStream oos;
		private ObjectInputStream ois;

		/**
		 * Se connecter avec un client
		 * @param socketClient socket du client avec laquelle on va communiquer
		 */
		public TraitementGeneral(Socket socketClient) {
			try {
				oos = new ObjectOutputStream(socketClient.getOutputStream());
				ois = new ObjectInputStream(socketClient.getInputStream());
			} catch (Exception e) {
				System.err.println("Demande non valable");
			}
		}

		@Override
		public void run() {
			// HDFS se connecte au DataNode : la commande détermine l'action à faire
			try {
				Object o = ois.readObject();
				String cmd = (String) o;
				
				switch (cmd) {
				case "CMD_WRITE":
					HDFSTraitementWrite();
					break;
				case "CMD_READ":
					HDFSTraitementRead();
					break;
				case "CMD_DELETE":
					HDFSTraitementDelete();
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new IllegalStateException("Erreur de syntaxe");
			}
		}
		
		/**
		 * Ecritures sous HDFS
		 */
		private void HDFSTraitementWrite() throws Exception {
		    String format = ois.readUTF();
		    System.out.println(format);
			// Stockage du fragment que HDFS vient d'envoyer
			Object f = ois.readObject();
			Fragment idFragment = (Fragment) f;
			Object lf = ois.readObject();
			String[] listeFragment = (String[]) lf;

			fragments.add(idFragment);

			// Ecrire le fragment sur le disque
			System.out.println(idFragment.getFichier());
			Format writer;
			if(format.equals("line")){
                writer = new LineFormat(idFragment.getFichier());
            }else {
                writer = new KVFormat(idFragment.getFichier());
            }
			writer.open(OpenMode.W);
			for (int i = 0; i < listeFragment.length; i++) {
				writer.write(KV.fromString(listeFragment[i]));
			}
			writer.close();

			oos.close();
		}
		
		
		/**
		 * Lectures sous HDFS
		 */
		private void HDFSTraitementRead() throws Exception {
			
			// Lecture du fichier que l'on souhaite
			String nom = (String) ois.readObject();
			
			// Envoi des fragments souhaités
			for (Fragment fragment : fragments) {
				if(fragment.getFichier().equals(nom)){
					oos.writeObject(fragment.getNumero());
					
					// Lire le contenu des fragments depuis le disque
						// Lecteur 
						Format reader = new KVFormat(fragment.getFichier());
						reader.open(OpenMode.R);
						List<String> lines = new ArrayList<String>(); // lignes lues
						KV line; // ligne lue
						
						while ((line = reader.read()) != null) {
							lines.add(line.k+KV.SEPARATOR+line.v);
						}
						
						// Convertir en tableau pour pouvoir l'envoyer
						String[] contenu = lines.toArray(new String[lines.size()]);
					
					oos.writeObject(contenu); 
				}
			}
			
			oos.close();
		}

		
		
		/**
		 * Suppressions sous HDFS
		 */
		private void HDFSTraitementDelete() throws Exception {
			
			// Lecture du fichier que l'on souhaite
			String nom = (String) ois.readObject();
			
			/// Envoi des fragments souhaités
			for (Fragment fragment : fragments) {
				if(fragment.getFichier().equals(nom)){
					// Suppression du contenu du fragment
					try{
			    		File file = new File(fragment.getFichier());
			    		file.delete();

			    	}catch(Exception e){
			    		e.printStackTrace();
			    	}
				}
			}

			oos.close();
		}
		
		
	}
}
