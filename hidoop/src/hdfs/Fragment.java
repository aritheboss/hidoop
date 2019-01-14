package hdfs;

import java.io.Serializable;

/**
 * Classe d'attributs d'un fragment
 */
public class Fragment implements Serializable {
	
	// nom du fichier source
	private String fichier;
	
	// adresse du serveur sur lequel a été fait le découpage
	private String adresseServeur;
	
	// numero du fragment
	private int numero;
	
	// numero du port
	private int port;
	
	
	// Getter et Setter
	public String getFichier() {
		return fichier;
	}
	
	public String getadresseServeur() {
		return adresseServeur;
	}
	
	public int getNumero() {
		return numero;
	}
	
	public int getPort() {
		return port;
	}

	
	/**
	 * Identification d'un fragment de fichier sur un serveur 
	 * @param fichier Le nom du fichier
	 * @param adresseServeur L'adresse du serveur
	 * @param numero Le numéro du fragment
	 * @param port Le port du serveur
	 */
	public Fragment(String fichier, String adresseServeur, int numero, int port) {
		super();
		this.fichier = fichier;
		this.adresseServeur = adresseServeur;
		this.numero = numero;
		this.port = port;
	}
	
	
	
}
