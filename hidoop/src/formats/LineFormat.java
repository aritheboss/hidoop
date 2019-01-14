package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class LineFormat implements Format {

    /** . */
	private static final long serialVersionUID = 1L;

	private String file;
    BufferedReader br;
    BufferedWriter bw;

    public LineFormat(String file) {
		this.file = file;
	}

	@Override
    public KV read() {
        KV kv = null;
    	String line;
        try {
            if ((line = this.br.readLine()) != null) {
                kv = new KV(this.getIndex() + "", line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kv;
    }

    @Override
    public void write(KV record) {
        try {
            this.bw.write(record.v + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long getIndex() {
        return 0;
    }

    @Override
    public void open(OpenMode mode) {
        try {
            if (mode == OpenMode.R) {
                this.br = new BufferedReader(new FileReader(this.file));
            }else{
                this.bw = new BufferedWriter(new FileWriter(this.file));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            if (this.br != null) {
                this.br.close();
            }
            if (this.bw != null) {
                this.bw.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setFname(String fname) {
    	this.file = "data/" + fname;
    }

    @Override
    public String getFname() {
        return this.file;
    }

}