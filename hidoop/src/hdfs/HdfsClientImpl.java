package hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import config.Project;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

/**
 * Classe de lecture, écriture et suppression d'un fichier par HDFS
 *
 */
public class HdfsClientImpl {


	private HdfsClientImpl() {}

	/**
	 * Création d'une connexion entre la client HDFS et le Serveur (NameNode)
	 * @param action celle à faire auprès du serveur (nameNode)
	 * @param fileName fichier dont on veut se servir
	 * @return socket qui permet d'accéder au node
	 */
	static Socket connectionAuNameNode(String action, String fileName) {

		Socket socket = null;
		try {
			// Connection HDFS-NameNode par un socket
			socket = new Socket(Project.ADRESSE_IP, Project.PORT_HDFS);		
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			// On indique que l'on est HDFS (et pas un chunk)
			oos.writeInt(0);
			// On envoie le nom du fichier
			oos.writeObject(fileName);
			// On écrit l'action à effectier
			oos.writeObject(action);

		} catch (IOException e) {
			throw new IllegalStateException("Erreur lors de l'accès au NameNode");
		}

		return socket;
	}
	
	
	/**
	 * Collecte de la liste des DataNode depuis le NameNode
	 * @param nameNode le nameNode
	 * @return liste des DataNode disponibles à partir du NameNode
	 */
	static String[][] collecteDesDataNodes(Socket nameNode) {

		String[][] nodes = null;
		try {
			ObjectInputStream ois = new ObjectInputStream(nameNode.getInputStream());
			
			nodes = (String[][]) ois.readObject();
		} catch (Exception e) {
			throw new IllegalStateException("Erreur dans la collecte des DataNode (Chunk)");
		}
		return nodes;
	}
	
	
	/**
	 * Supprime les fragments d'un fichier sur les dataNodes collectés
	 * @param nodes dataNodes collectés
	 * @param hdfsFname fichier dont on veut supprimer les fragments
	 */
	static void supprimerFragment(String[][] nodes, String hdfsFname) {
		Socket socket = null;
			try {
				for(int i=0; i<=nodes.length;i++){
					// Connection au dataNode
					String[] dataNode = nodes[i];
					socket = new Socket(dataNode[0], Integer.parseInt(dataNode[1]));
					
					// Création de l'objet puis écriture sur le socket 
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					// Envoi de la commande de suppression et du fichier à supprimer au dataNode
					oos.writeObject("CMD_DELETE");
					oos.writeObject(hdfsFname);
					
					socket.close();
				}
			} catch (IOException | NumberFormatException e) {
				e.printStackTrace();
			}
		}


	
	/**
	 * Lecture d'un fichier sous le format KV
	 * @param type le type du fichier
	 * @param fichier le fichier source
	 * @return Tableau de KV associé au fichier source
	 */
	static KV[] lireFichier(Type type, String fichier) {
		// Liste pour contenir le résultat
		List<KV> resultat = new ArrayList<>();
		
		// Curseur se chargeant de la lecture du fichier
		Format curseur;
		// 2 types de curseurs sont possibles, il faut prendre le bon
		if (type == Format.Type.KV) { 
	     	curseur = new KVFormat(fichier);
		} else {
			curseur = new LineFormat(fichier);
		}

		curseur.open(OpenMode.R);
		//curseur.setFname(fichier);

		
		// Lecture du fichier
		KV donnees;
		while ((donnees = curseur.read()) != null) {
			// Ajout au resultat
			resultat.add(donnees);
		}
		
		//curseur.close();

		// Conversion de la liste (collection) en tableau (array)
		return resultat.toArray(new KV[resultat.size()]);
	}

	
	/**
	 * Découpage d'un fichier en fragments (de taille identique)
	 * @param fichier fichier à découper
	 * @param nbFragmentsSouhaite nombre de fragments souhaité
	 * @return découpage en fragments
	 */
	static String[][] decouperEnFragments(KV[] fichier, int nbFragmentsSouhaite) {
		// Tableau pour contenir le découpage
		Integer colonnes = (fichier.length / nbFragmentsSouhaite);
		String[][] tab = new String[nbFragmentsSouhaite][colonnes];

		// Découpage en fragments par parcours du fichier
		for (int i = 0; i < fichier.length; i++) {
			// Utilisation du moduloS
			Integer ligne = i % tab.length;
			Integer colonne = i / tab.length;
			tab[ligne][colonne] = fichier[i].k + KV.SEPARATOR + fichier[i].v;
		}

		// Résultat du découpage
		return tab;
	}
	

	/**
	 * Envoyer un fragment sur un dataNode
	 * @param idFragment attributs du fragment
	 * @param fragment celui à envoyer
	 */
	static void ecrireFragmentDataNode(Fragment idFragment, String[] fragment, Format.Type fmt) {
		Socket socket = null;

			try {
				// Connection au dataNode
				String server = idFragment.getadresseServeur();
				Integer port = idFragment.getPort();
				System.out.println(server + " : " + port);
				socket = new Socket(server, port);

				// Création de l'objet puis écriture sur le socket 
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());


				// Envoi de la commande de suppression et du fichier à écrire sur le dataNode
				oos.writeObject("CMD_WRITE");
				if(fmt.equals(Type.LINE)){
                    oos.writeUTF("line");
                }else{
                    oos.writeUTF("kv");
                }
				oos.writeObject(idFragment);
				oos.writeObject(fragment);
				
				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				socket.close();
				
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalStateException("Erreur lors de l'écriture du fragment sur le DataNode");
			} 
	}

	

	/**
	 * 
	 * @param nodes
	 * @param hdfsFname
	 * @return
	 */
	static HashMap<Integer, String[]> collecteDesFragments(String[][] nodes, String hdfsFname) {
		Socket socket = null;
		HashMap<Integer, String[]> fragments = new HashMap<Integer, String[]>();
		
		try {
			for(int i=0; i<nodes.length;i++){
				// Connection au dataNode
				socket = new Socket(nodes[i][0], Integer.parseInt(nodes[i][1]));
				
				// Création de l'objet puis écriture sur le socket 
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				
				// Envoi de la commande de lecture et lecture du fichier sur le socket
				oos.writeObject("CMD_READ");
				oos.writeObject(hdfsFname);
				ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
					
				// numero du fragment
				int idFrag;
				String[] leFragment;
				try {
					idFrag = (Integer) ois.readObject();
					leFragment = (String[]) ois.readObject();
					fragments.put(idFrag, leFragment);
				}catch(EOFException e){
					
				}catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
			socket.close();
			return fragments;

		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException("Erreur lors de l'accès au NameNode (Serveur)");
		}

	}
	

}