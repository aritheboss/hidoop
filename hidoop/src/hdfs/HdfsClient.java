/* une PROPOSITION de squelette, incompl��te et adaptable... */

package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.Project;
import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

public class HdfsClient {

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}
	
	
	/**
	 * HDFS Supprimer un fichier 
	 * @param hdfsFname nom du fichier à supprimer
	 */
	public static void HdfsDelete(String hdfsFname) {
		
		// Se connecter au au nameNode
		Socket nameNode = HdfsClientImpl.connectionAuNameNode("CMD_DELETE", hdfsFname);

		// Liste des endroits (machines) contenant les morceaux du fichier
		String[][] nodes = HdfsClientImpl.collecteDesDataNodes(nameNode);

		// Supprimer les fragments sur les dataNodes
		HdfsClientImpl.supprimerFragment(nodes, hdfsFname);

	}
	
	
	/**
	 * HDFS Ecrire un fichier
	 * @param fmt le format du fichier
	 * @param localFSSourceFname le fichier source
	 * @param repFactor le nombre de répartition que l'on veut
	 */
	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {

		// Lecture du fichier sous le format KV

			//[] listeFragments;
			// Ecrire les fragments sur chaque DataNode
		try {
			KV[] fichier = HdfsClientImpl.lireFichier(fmt, localFSSourceFname);

			// Connection au nameNode
			Socket socket = new Socket(Project.ADRESSE_IP, Project.PORT_HDFS);

			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());

			// On indique que l'on est HDFS (et pas un chunk)
			oos.writeInt(0);

			// On envoie le nom du fichier
			oos.writeObject(localFSSourceFname);

			// On écrit l'action à effectuer
			oos.writeObject("CMD_WRITE");

			// Collecte des dataNodes dispos depuis le namenode
			String[][] nodes = HdfsClientImpl.collecteDesDataNodes(socket);

			// Découpage en fragments du fichier
			String[][] fragments = HdfsClientImpl.decouperEnFragments(fichier, nodes.length);

			// Distribution des fragments sur les serveurs disponibles
			Fragment fragment; // Le fragment qui sera envoyé
			for (int i = 0; i < nodes.length; i++) {
				String[] dataNode = nodes[i];
				fragment = new Fragment(localFSSourceFname.replace(".txt", i+".txt"), dataNode[0], i, Integer.valueOf(dataNode[1]));
				HdfsClientImpl.ecrireFragmentDataNode(fragment, fragments[i], fmt);

				// Enregistrer ce que chaque dataNode contient comme fragment puis écriture
				oos.writeObject(fragment);
			}
			oos.close();
		}catch (IOException e){
			e.printStackTrace();
		}
		// Connection terminée
	}

	
	
	
	/**
	 * HDFS Lire un fichier
	 * @param hdfsFname le fichier à lire
	 * @param localFSDestFname le fichier local ?? cr??er
	 */
	public static void HdfsRead(String hdfsFname, String localFSDestFname) {
				
		// Connection au nameNode
		Socket nameNode = HdfsClientImpl.connectionAuNameNode("CMD_READ", hdfsFname);

		// Liste des endroits (machines) contenant les morceaux du fichier
		String[][] nodes = HdfsClientImpl.collecteDesDataNodes(nameNode);

		// Collecte des fragments aux différents endroits (machines) 
		HashMap<Integer, String[]> fragments = HdfsClientImpl.collecteDesFragments(nodes, hdfsFname);

		
		// placement de leur concaténation dans un fichier local
			
			// Initialisation
			List<String[]> fragList = new ArrayList<>();
			// On remplie la liste avec des valeurs vierges
			for (int i = 0; i < fragments.size(); i++) {
				fragList.add(new String[0]);
			}

			// Remplir la liste dans l'ordre		
			for (Map.Entry<Integer, String[]> e : fragments.entrySet()) {
				fragList.set(e.getKey(), e.getValue());
			}

			// Parcourt la liste dans l'ordre
			StringBuilder concatenation = new StringBuilder();
			for(int i = 0; i < fragList.size(); i++) {
				for (String ligne : fragList.get(i)) {
					concatenation.append(fragList.get(i));
				}
			}	
			localFSDestFname = concatenation.toString();
			System.out.println(localFSDestFname);

	}
	
	
	/** 
	 * FONCTION PRINCIPALE 
	 */

	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>

		try {
			if (args.length < 2) {
				usage();
				return;
			}

			Thread.sleep(1000);
			
			switch (args[0]) {
			case "read":
				HdfsRead(args[1], null);
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			case "write":
				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line")) {
					fmt = Format.Type.LINE;
				} else if (args[1].equals("kv")) {
					fmt = Format.Type.KV;
				} else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], 1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
