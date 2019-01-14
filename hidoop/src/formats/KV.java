package formats;

public class KV {

	public static final String SEPARATOR = "<->";
	
	public String k;
	public String v;
	
	public KV() {}
	
	public KV(String k, String v) {
		super();
		this.k = k;
		this.v = v;
	}

	public String toString() {
		return "KV [k=" + k + ", v=" + v + "]";
	}

	public static KV fromString(String s) {
		KV retour;
		if (s != null) {
			String[] parts = s.split(KV.SEPARATOR);
			retour = new KV(parts[0], parts[1]);
		}
		else {
			retour = null;
			System.out.println("fromString -> Chaine de caractère vide");
		}

		return retour;

	}

}
