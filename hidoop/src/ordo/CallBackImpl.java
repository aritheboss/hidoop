package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack{

	private Semaphore semaphore;
	private static final long serialVersionUID = 1L;

	protected CallBackImpl() throws RemoteException {
		super();
		semaphore = new Semaphore(0);
	}

	public void increment() throws RemoteException {
        semaphore.release();
    }

    public void decrement() throws RemoteException, InterruptedException {
       semaphore.acquire();
    }

} 
