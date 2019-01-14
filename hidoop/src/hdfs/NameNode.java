package hdfs;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class NameNode {

	public ArrayList<String> chunks;
	public ConcurrentHashMap<String, List<Fragment>> fichiers;

	/** 
	 * Initialisation du serveur 
	 */
	private NameNode() {
		chunks = new ArrayList<>();
		fichiers = new ConcurrentHashMap<>();
	}

	/** 
	 * Lancement du serveur 
	 */
	private void launch() {
		// serveurHDFS
		ServerSocket socketHDFS;

		try {
			// écoute sur le port HDFS
			socketHDFS = new ServerSocket(8080);

			// Création d'un thread pour chaque demande 
			while (true) {
				Thread t = new Thread(new TraitementGeneral(socketHDFS.accept(), this));
				t.start();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("javadoc")
	public static void main(String[] zero) {
		new NameNode().launch();
	}
	
	
	

	/**
	 * Gérer les chunks, du côté des DataNodes
	 */
	private class ChunkTraitement {

		/** Création des outils d'écriture et de lecture sur la socket */
		private ObjectOutputStream oos;
		private ObjectInputStream ois;
		private NameNode nn;
		
		public ChunkTraitement(ObjectOutputStream oos, ObjectInputStream ois, NameNode nn) {
			this.oos = oos;
			this.ois = ois;
			this.nn = nn;
		}

		public void launch() throws Exception {
			// lecture de l'adresse du chunk et du numéro de port
			String adresseIp = (String) ois.readObject();
			Integer numeroPort = (Integer) ois.readObject();

			// rajout à la liste des chunks
			System.out.println(nn.chunks.size());
			nn.chunks.add(adresseIp+":"+numeroPort);
			System.out.println("ChunkTraitement -> le chunk a été ajouté");
			System.out.println(nn.chunks.size());
		}
	}

	
	
	/**
	 * Gérer les requêtes HDFS, du côté du Client
	 */
	private class HDFSTraitement {

		/** Outils de lecture et d'écriture sur la socket */
		private ObjectOutputStream oos;
		private ObjectInputStream ois;
		
		/** Nom du fichier */
		private String fileName;

		public HDFSTraitement(ObjectOutputStream oos, ObjectInputStream ois) {
			this.oos = oos;
			this.ois = ois;
		}

		public void launch() throws Exception {
			// quel est le nom du fichier?
			this.fileName = (String) ois.readObject();
			
			// quelle est la commande souhaitée?
			String cmd = (String) ois.readObject();
			switch (cmd) {
			case "CMD_DELETE":
				HDFSTraitementDelete();
				break;
			case "CMD_WRITE":
				HDFSTraitementWrite();
				break;
			case "CMD_READ":
				HDFSTraitementRead();
				break;
			default:
				throw new IllegalStateException("Commande non reconnue");
			}
		}

		
		/**
		 * Suppression sous HDFS
		 */
		private void HDFSTraitementDelete() throws Exception {
			// quels sont les endroits qui contiennent des fragments?
			// Récupération de tous les fragments disponibles
			List<Fragment> fragments = fichiers.get(this.fileName);
			String[][] listeFragments = new String[fragments.size()][2];
			
			for (int i=0; i<=fragments.size(); i++) {
				listeFragments[i][0] = fragments.get(i).getadresseServeur();
				listeFragments[i][1] = Integer.toString(fragments.get(i).getPort());
			}
			// Ecriture de ces fragments
			oos.writeObject(listeFragments);
			
			// suppression totale du fichier
			fichiers.remove(this.fileName);
			
			oos.close();
		}
		
		
		
		/**
		 * Ecritures sous HDFS
		 */
		private void HDFSTraitementWrite() throws Exception {
			// Récupération de la liste des chunks
			String[][] listeChunks = new String[chunks.size()][2]; 
			int i = 0;
			for (String entry : chunks) {
				listeChunks[i][0] = entry.split(":")[0];
				listeChunks[i][1] = entry.split(":")[1];
				i++;
			}
			
			// Ecriture de la liste des chunks
			oos.writeObject(listeChunks);
			
			// Contruction de la liste des fragments
			Object obj; 
			List<Fragment> fragments = new ArrayList<>();
			try {
				while ((obj = ois.readObject()) != null) {
					if (obj instanceof Fragment) {
						fragments.add((Fragment) obj);
					}
				}

			}catch (Exception e ){
				e.printStackTrace();
			}
			// Ajout à la liste principale
			fichiers.put(this.fileName, fragments);
			
			oos.close();
		}

		
		
		/**
		 * Traitement des demandes de lecture de HDFS
		 */
		private void HDFSTraitementRead() throws Exception {
			// Récupération de tous les fragments disponibles

			List<Fragment> fragments = fichiers.get(this.fileName);
			String[][] listeFragments = new String[fragments.size()][2];
			
			for (int i=0; i<fragments.size(); i++) {
				listeFragments[i][0] = fragments.get(i).getadresseServeur();
				listeFragments[i][1] = Integer.toString(fragments.get(i).getPort());
			}
			
			// Ecriture de tous ces fragments
			oos.writeObject(listeFragments);
		}

		
	}

	
	
	/**
	 * Traiter la demande d'un client (HDFS ou chunk)
	 */
	private class TraitementGeneral implements Runnable {
		/** Outils d'écriture et lecture sur la socket */
		private ObjectOutputStream oos;
		private ObjectInputStream ois;
		private NameNode nn;

		/**
		 * Se connecter avec un client
		 * @param socketClient socket du client avec laquelle on va communiquer
		 */
		public TraitementGeneral(Socket socketClient, NameNode nn) {
			try {
				oos = new ObjectOutputStream(socketClient.getOutputStream());
				ois = new ObjectInputStream(socketClient.getInputStream());
				this.nn = nn;
			} catch (Exception e) {
				System.err.println("Demande non valable");
			}
		}

		@Override
		public void run() {
			// On lit le premier entier pour voir si c'est HDFS ou un chunk qui se connecte

			try {
				switch (ois.readInt()) {
				case 0:
					new HDFSTraitement(oos, ois).launch();
					break;
				case 1:
					new ChunkTraitement(oos, ois, nn).launch();
					break;
				default:
					throw new IllegalStateException("Le client est inconnu");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

}
