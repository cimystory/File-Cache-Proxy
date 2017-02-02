import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;  
import java.rmi.registry.LocateRegistry; 
import java.io.*;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

public class Server extends UnicastRemoteObject implements FileInterface {
	private static String rootDir;
	private static int port;
	private static HashMap<String, String> version;
	private static final int CHUNKSIZE=1000000;
	public Server() throws RemoteException{
		super();
	}

	public boolean deleteFile(String fileName){
		try{ 
			File file = new File(rootDir, fileName);
			System.err.println("delete fileName");
			if(version.containsKey(fileName)){
				version.put(fileName, "deleted");
			}
			return file.delete();
		} catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}

	public String[] checkFile(String path){
		System.err.println("checkFile!!!"+path);
		LinkedList list = new LinkedList<String>();
        StringBuilder sb = new StringBuilder();
        String[] str = path.split("/");
        if(str.length>1){
            for(int i=0; i<str.length; i++){
                if(str[i].equals(".")) continue;
                if(str[i].equals("..")){
                    if(list.size()==0) list.add(str[i]);
                    else list.remove(list.size()-1);
                }else{
                    list.add(str[i]);
                }
            }
            for(int i=0; i<list.size(); i++){
                sb.append(list.get(i));
                if(i!=(list.size()-1)) sb.append("/");
            }
            int i;
            for(i=path.length()-1; i>=0; i--){
                if(path.charAt(i)=='/') break;
            }
            path = sb.toString();
        }
		String[] res = new String[5];
		res[3]="true";
		int len = path.length();
		int c = 0, i=0;
		while(i<len){
			if(path.charAt(i)!='.' && path.charAt(i)!='/'){
				i++;
				continue;
			}
			if(path.charAt(i)=='.'){
				if(i+1<len && path.charAt(i+1)=='/') i+=2;
				if(i+2<len && path.charAt(i+1)=='.' && path.charAt(i+2)=='/'){
					i+=3;
					c--;
					if(c<0) {
						res[3]="false";
						break;
					}
				}
			}else{ // path.charAt(i)=='/'
				i++;
				c++;
			}
		}

		try{
			if(res[3].equals("false")) return res;
			File file = new File(rootDir, path);
			res[0]=file.exists()?"true":"false";
			res[1]=file.isDirectory()?"true":"false";
			if(res[0].equals("true") && res[1].equals("false")){
				if(!version.containsKey(path)){
					String timeStamp = new java.text.SimpleDateFormat("HHmmss").format(new Date());
					version.put(path, timeStamp);
				}
				res[2]=version.get(path);
				System.err.println(path+"'s version: "+res[2]);
				res[4]=Integer.toString((int) file.length());
				return res;
			}
			res[2]="000000"; 
			res[4]="0";
			return res;
		} catch(Exception e){
			e.printStackTrace();
			return(null);
		}
	}
	
	public byte[] downloadFile(String fileName, int i){
		try {
			File file = new File(rootDir, fileName);
			System.err.println("now trying to download file: "+file.getAbsoluteFile()+"part"+i);
			int filesize = (int)file.length();
			int left = filesize-CHUNKSIZE*i;
			int buffersize = (left>=CHUNKSIZE?CHUNKSIZE:left);
			byte buffer[] = new byte[buffersize];
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			raf.seek(CHUNKSIZE*i);
			raf.read(buffer);
			System.err.println("reading at offset: "+CHUNKSIZE*i+" buffersize:"+buffersize);
			System.err.println("downloaded "+buffersize+"byte");
			return buffer;
		} catch(Exception e){
			e.printStackTrace();
			return(null);
		}
	}

	public void uploadFile(String fileName, byte[] fileData, String time, int i){
		System.err.println("uploadFile: "+fileName+" part"+i);
		String[] str = fileName.split("/");
        if(str.length>1 && i==0){
            int j;
            for(j=fileName.length()-1; j>=0; j--){
                if(fileName.charAt(j)=='/') break;
            }
            String p =fileName.substring(0,j);
            File parent = new File(rootDir+"/"+p);
            System.err.println("parent: "+ p);
            if(!parent.exists()){
                System.err.println("dir not exist! create dir: "+parent.mkdirs());
            }
        }
        try {
            File file = new File(rootDir, fileName);
            if(i==0 && file.exists()) file.delete(); // delete old version
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(raf.length());
            raf.write(fileData);
            if(i==0) version.put(fileName, time);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	public static void main(String args[]) {
		if (args.length != 2) {
			throw new IllegalArgumentException("Number of expected args are 2");
		}
		version = new HashMap<String, String>();
		port = Integer.parseInt(args[0]);
		rootDir = String.valueOf(args[1]);
		String str = "//127.0.0.1:"+args[0]+"/Server";
		try {
			FileInterface fi = new Server();
			LocateRegistry.createRegistry(port);
			Naming.rebind(str, fi);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}