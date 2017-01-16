import java.rmi.Remote;
import java.rmi.RemoteException;

public interface FileInterface extends Remote {
   public byte[] downloadFile(String fileName, int i) throws RemoteException;
   public String[] checkFile(String fileNme) throws RemoteException;
   public void uploadFile(String fileName, byte[] fileData, String time, int i) throws RemoteException;
   public boolean deleteFile(String fileName) throws RemoteException;
}
