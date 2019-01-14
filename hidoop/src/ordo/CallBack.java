package ordo;
import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.Semaphore;

public interface CallBack extends Remote  {
	
	public void increment() throws RemoteException;
	
	public void decrement() throws RemoteException, InterruptedException;
	

}
