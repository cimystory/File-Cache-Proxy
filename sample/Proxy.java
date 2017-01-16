import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.*;
import java.rmi.*;

class Proxy {
	private static String serverIP, cacheDir;
    private static int serverPort, cacheSize;
    private static int cacheUsed;
    public static LinkedList<String> cacheQueue;
    public static HashMap<String, Integer> reading;
    public static HashMap<String, String> notUse;
    private static final int CHUNKSIZE=1000000;
    private static FileInterface fi;

	private static class cacheFile {
        String name;
        boolean closed;
        boolean canWrite;
        boolean isDir;
        RandomAccessFile raf;
        
        public cacheFile() {
            this.closed = false;
        }
        
        public cacheFile(RandomAccessFile raf) {
            this.raf = raf;
        }
        
        public cacheFile(String name, boolean closed,
                         boolean canWrite, boolean isDir, RandomAccessFile raf) {
            this.name = name;
            this.closed = closed;
            this.canWrite = canWrite;
            this.isDir = isDir;
            this.raf = raf;
        }
        
    }

    // make sure parent direcotry exist
    private static String parsePath(String path){
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
            String p=path.substring(0,i);
            File parent = new File(cacheDir+"/"+p);
            System.err.println("parent: "+ p);
            if(!parent.exists()){
                System.err.println("dir not exist! create dir: "+parent.mkdirs());
            }
            return sb.toString();
        }
        return path;
    }

    // check if cache has enough space, evict files if necessary
    private static synchronized boolean checkCache(){
        System.err.println("before check: cacheUsed="+cacheUsed);
        while(cacheUsed>cacheSize && notUse.size()>0){
            for(int i=cacheQueue.size()-1; i>=0; i--){
                if(notUse.containsKey(cacheQueue.get(i))){
                    File file = new File(cacheDir, notUse.get(cacheQueue.get(i)));
                    int len = (int) file.length();
                    System.err.println("remove "+notUse.get(cacheQueue.get(i)));
                    file.delete();
                    cacheUsed-=len;
                    notUse.remove(cacheQueue.get(i));
                    //updateQueue(null, i);
                    break;
                }
            }
        }
        System.err.println("after check: cacheUsed="+cacheUsed);
        if(cacheUsed<=cacheSize) return true;
        System.err.println("can't make enough space");
        return false;
    }

    // update queue (both add and remove)
    private static synchronized void updateQueue(String name, int op){
        System.err.println("files in cacheQueue:");
        for(int i=0; i<cacheQueue.size(); i++){
            System.err.printf(cacheQueue.get(i)+" ");
        }
        if(op==-1){ // put q in the queue
            cacheQueue.add(0, name);
        }else{ // evict file out of queue
            cacheQueue.remove(op);
        }
        System.err.printf("-->");
        for(int i=0; i<cacheQueue.size(); i++){
            System.err.printf(cacheQueue.get(i)+" ");
        }
        System.err.println();
    }

	
	private static class FileHandler implements FileHandling {
		private static final int OFFSET = 2000;

		CopyOnWriteArrayList<cacheFile> fdTable= new CopyOnWriteArrayList<cacheFile>();
		public int open( String path, OpenOption o ) {
            System.err.printf("Open(%s,", path);
            System.err.print(o);
            System.err.printf(")\n");
	        // String name = "//"+serverIP+":"+serverPort+"/Server";
            String[] serverState = {"false", "false", "000000", "false", "0"};
            //FileInterface fi = null;
            try{
                //fi = (FileInterface) Naming.lookup(name);
                serverState=fi.checkFile(path);
            } catch(Exception e){
                e.printStackTrace();
            }
            if(serverState[3].equals("false")) return Errors.EPERM;
            path=parsePath(path); // create parent dirctory if necessary
            // delete cache that has been deleted before
            if(serverState[0].equals("false") && notUse.containsKey(path)){ 
                File f = new File(cacheDir, notUse.get(path));
                int len = (int) f.length();
                System.err.println("delete 2 "+f.getName());
                f.delete();
                cacheUsed -= len;
                notUse.remove(path);
                for(int i=0; i<cacheQueue.size(); i++){
                    if(cacheQueue.get(i).equals(path)) {
                        updateQueue(null, i); // delete from cacheQueue
                        break;
                    }
                }
            }
            File f = new File(cacheDir, path+"#"+serverState[2]);
            boolean fileExist = f.exists();
            boolean fileisDir = f.isDirectory();
            // delete old version
            if(notUse.containsKey(path)){
                if(!notUse.get(path).equals(path+"#"+serverState[2])){
                    File ff = new File(cacheDir, notUse.get(path));
                    int len = (int) ff.length();
                    System.err.println("delete 3 "+ff.getName());
                    ff.delete();
                    cacheUsed-=len;
                    notUse.remove(path);
                }
            }
            int filesize = Integer.parseInt(serverState[4]);
            switch (o) {
                case READ:
                    try {
                        if (!fileExist) {
                            if(serverState[0].equals("false")) return Errors.ENOENT; // file doesn't exist
                            if(serverState[1].equals("false")){ // not a directory
                                if(filesize>cacheSize) return Errors.ENOMEM;
                                cacheUsed+=filesize;
                                if(!checkCache()){
                                    cacheUsed-=filesize;
                                    return Errors.ENOMEM;
                                }
                                System.err.println("cacheUsed new value from adding "+path+"("+filesize+") :"+cacheUsed);
                                int n=filesize/CHUNKSIZE;
                                if(filesize%CHUNKSIZE!=0) n++;
                                int left = filesize;
                                File new_file = new File(cacheDir, path+"#"+serverState[2]);
	                			BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new_file));
                                for(int i=0; i<n; i++){
                                    byte[] filedata = fi.downloadFile(path, i);
    	                			output.write(filedata, 0, filedata.length);
    	                			output.flush();
                                    left-=CHUNKSIZE;
                                }
	                			output.close();
	                		}
	                	}
	                } catch(Exception e) {
	                	e.printStackTrace();
	                }
                    
                    try {
                        File f1 = new File(cacheDir, path+"#"+serverState[2]);
                        RandomAccessFile file = new RandomAccessFile(f1, "r");
                        cacheFile cf = new cacheFile(file);
                        cf.name = path+"#"+serverState[2];
                        cf.closed = false;
                        cf.canWrite = false;
                        cf.isDir = false;
                        fdTable.add(cf);
                        if(reading.containsKey(cf.name)){
                            reading.put(cf.name, reading.get(cf.name)+1);
                        }else{
                            reading.put(cf.name, 1);
                        }
                        if(cacheQueue.size()>0 && cacheQueue.get(0).equals(path)){}
                        else{
                            for(int i=0; i<cacheQueue.size(); i++){
                                if(cacheQueue.get(i).equals(path)){
                                    updateQueue(null, i);
                                    break;
                                }
                            }
                            updateQueue(path, -1);
                        }
                        return (fdTable.size()-1+OFFSET);
                    } catch (FileNotFoundException e) {
                        // if it's a directory
                        if (fileisDir||serverState[1].equals("true")) {
                            File f2 = new File(cacheDir,path);
                            cacheFile cf = new cacheFile();
                            cf.name = path;
                            cf.closed = false;
                            cf.canWrite = false;
                            cf.isDir = true;
                            fdTable.add(cf);
                            return (fdTable.size()-1+OFFSET);
                        }
                        return Errors.ENOENT;
                    } catch (IllegalArgumentException e) {
                        return Errors.EINVAL;
                    }

                case WRITE:
                    if (!fileExist) {
                        try {
                            if(serverState[0].equals("false")) return Errors.ENOENT;
                            if(serverState[1].equals("true")) return Errors.EISDIR;
                            if(filesize>cacheSize) return Errors.ENOMEM;
                            cacheUsed+=filesize;
                            if(!checkCache()){
                                cacheUsed-=filesize;
                                return Errors.ENOMEM;
                            }
                            System.err.println("cacheUsed new value from adding "+path+"("+filesize+") :"+cacheUsed);
                            int n=filesize/CHUNKSIZE;
                            if(filesize%CHUNKSIZE!=0) n++;
                            int left = filesize;
                            File new_file = new File(cacheDir, path+"#"+serverState[2]);
                            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new_file));
                            for(int i=0; i<n; i++){
                                byte[] filedata = fi.downloadFile(path, i);
                                output.write(filedata, 0, filedata.length);
                                output.flush();
                                left-=CHUNKSIZE;
                            }
                            output.close();
                            // System.err.println
                            notUse.put(path, path+"#"+serverState[2]);
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                    }  
                    // copy file path+#+time to path+#+time+w
                    try{
                        File from = new File(cacheDir, path+"#"+serverState[2]);
                        File to = new File(cacheDir, path+"#"+serverState[2]+"#w");
                        cacheUsed+=from.length();
                        if(!checkCache()){
                                cacheUsed-=filesize;
                                return Errors.ENOMEM;
                        }
                        System.err.println("cacheUsed new value from copying "+path+"("+filesize+") :"+cacheUsed);
                        Files.copy(from.toPath(), to.toPath()); 
                    }catch (Exception ex){
                        System.err.println("copy failed!");
                    }          
                    try {
                        File f1=new File(cacheDir, path+"#"+serverState[2]+"#w");
                        RandomAccessFile file = new RandomAccessFile(f1, "rw");
                        cacheFile cf = new cacheFile(file);
                        cf.name = path+"#"+serverState[2]+"#w";
                        cf.closed = false;
                        cf.canWrite = true;
                        cf.isDir = false;
                        fdTable.add(cf);
                        if(cacheQueue.size()>0 && cacheQueue.get(0).equals(path)){}
                        else{
                            for(int i=0; i<cacheQueue.size(); i++){
                                if(cacheQueue.get(i).equals(path)){
                                    updateQueue(null, i);
                                    break;
                                }
                            }
                            updateQueue(path, -1);
                        }
                        return (fdTable.size()-1+OFFSET);
                    } catch (IllegalArgumentException e) {
                        return Errors.EINVAL;
                    } catch (FileNotFoundException e) {
                    	return Errors.ENOENT;
                    }
                    //return Errors.EINVAL;

                case CREATE:
                    if (fileExist && fileisDir) {
                        return Errors.EISDIR;
                    }
                    if(!fileExist){
                    	try {
                    		if(serverState[0].equals("true")){ // if file exist on the server
	                    		if(serverState[1].equals("true")) return Errors.EISDIR;
                                if(filesize>cacheSize) return Errors.ENOMEM;
                                cacheUsed+=filesize;
                                if(!checkCache()){
                                    cacheUsed-=filesize;
                                    return Errors.ENOMEM;
                                }
                                System.err.println("cacheUsed new value from adding "+path+"("+filesize+") :"+cacheUsed);
                                int n=filesize/CHUNKSIZE;
                                if(filesize%CHUNKSIZE!=0) n++;
                                int left = filesize;
                                File new_file = new File(cacheDir, path+"#"+serverState[2]);
                                BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(new_file));
                                for(int i=0; i<n; i++){
                                    byte[] filedata = fi.downloadFile(path, i);
                                    output.write(filedata, 0, filedata.length);
                                    output.flush();
                                    left-=CHUNKSIZE;
                                }
                                output.close();
                                // copy file path+#+time to path+#+time+w
                                notUse.put(path, path+"#"+serverState[2]);
                                cacheUsed+=filesize;
                                if(!checkCache()){
                                        cacheUsed-=filesize;
                                        return Errors.ENOMEM;
                                }
                                System.err.println("cacheUsed new value from copying "+path+"("+filesize+") :"+cacheUsed);
                                File to=new File(cacheDir, path+"#"+serverState[2]+"#w");
                                Files.copy(new_file.toPath(), to.toPath());  
                    		}
                    	} catch(Exception e) {
                    		e.printStackTrace();
                    	}
                    }
                    try {
                        String new_path = path+"#"+serverState[2]+"#w";
                        File f1 = new File(cacheDir, new_path);
                        if(!f1.exists()){
                            String timeStamp = new java.text.SimpleDateFormat("HHmmss").format(new Date());
                            new_path = path+"#"+timeStamp+"#w";
                            f1 = new File(cacheDir, new_path);
                        }
                        RandomAccessFile file = new RandomAccessFile(f1, "rw");
                        cacheFile cf = new cacheFile(file);
                        cf.name = new_path;
                        cf.closed = false;
                        cf.canWrite = true;
                        cf.isDir = false;
                        fdTable.add(cf);
                        if(cacheQueue.size()>0 && cacheQueue.get(0).equals(path)){}
                        else{
                            for(int i=0; i<cacheQueue.size(); i++){
                                if(cacheQueue.get(i).equals(path)){
                                    updateQueue(null, i);
                                    break;
                                }
                            }
                            updateQueue(path, -1);
                        }
                        return (fdTable.size()-1+OFFSET);
                    } catch (IllegalArgumentException e) {
                        return Errors.EINVAL;
                    } catch (FileNotFoundException e) {
                    	return Errors.ENOENT;
                    }
                    //return Errors.EINVAL;

                case CREATE_NEW:
                    if (serverState[0].equals("true")) {
                        return Errors.EEXIST;
                    }
                    try {
                        String timeStamp = new java.text.SimpleDateFormat("HHmmss").format(new Date());
                        File f1 = new File(cacheDir, path+"#"+timeStamp+"#w");
                        RandomAccessFile file = new RandomAccessFile(f1, "rw");
                        cacheFile cf = new cacheFile(file);
                        cf.name = path+"#"+timeStamp+"#w";
                        cf.closed = false;
                        cf.canWrite = true;
                        cf.isDir = false;
                        fdTable.add(cf);
                        return (fdTable.size()-1+OFFSET);
                    } catch (IllegalArgumentException e) {
                        return Errors.EINVAL;
                    } catch (FileNotFoundException e) {
                    	return Errors.ENOENT;
                    }
                    //return Errors.EINVAL;
                default:
                    return Errors.EINVAL;
            }
		}

		public int close( int fd ) {
			System.err.printf("Close!\n");
            fd = fd-OFFSET;
            cacheFile cf = fdTable.get(fd);
            String[] str = cf.name.split("#");
            if (cf.closed) {
                return Errors.EBADF;
            }
            if (cf.isDir) {
                cf.closed = true;
            } else {
                RandomAccessFile f = cf.raf;
                try {
                    f.close();
                    cf.closed = true;
                } catch (IOException e) {
                    System.err.printf("Error in closing RandomAccessFile!\n");
                }
            }
            System.err.println("close: "+cf.name);
            int i=0;
            if(cacheQueue.size()>0 && cacheQueue.get(i).equals(str[0])){}
            else{
                for(; i<cacheQueue.size(); i++){
                    if(cacheQueue.get(i).equals(str[0])){
                        updateQueue(null, i);
                        break;
                    }
                }
                updateQueue(str[0], -1);
            }
            if(str.length==2){ // close read
                reading.put(cf.name, reading.get(cf.name)-1);
                if(reading.get(cf.name)==0){ // no clients using this file now
                    reading.remove(cf.name);
                    if(notUse.containsKey(str[0])){ // delete outdated cache file
                        String s = notUse.get(str[0]);
                        String[] tmp = s.split("#");
                        int c = str[1].compareTo(tmp[1]);
                        if(c<0){ // delete this file
                            File file = new File(cacheDir, cf.name);
                            cacheUsed -= file.length();
                            System.err.println("delete 4 "+file.getName());
                            file.delete();
                        }
                        if(c>0){ // delete the other
                            File file = new File(cacheDir, s);
                            cacheUsed -= file.length();
                            System.err.println("delete 5 "+file.getName());
                            file.delete();
                            notUse.put(str[0], cf.name);
                        }
                    }else{
                        notUse.put(str[0], cf.name);
                    }
                }
            } else{ // close write
                String name = "//"+serverIP+":"+serverPort+"/Server";
                String[] serverState = {"false", "false", "000000", "false", "0"};
                try{
                    FileInterface fi = (FileInterface) Naming.lookup(name);
                    File file = new File(cacheDir, cf.name);
                    RandomAccessFile raf = new RandomAccessFile(file, "r");
                    int filesize = (int) file.length();
                    // byte[] buffer = new byte[filesize];
                    // BufferedInputStream input = new BufferedInputStream(new FileInputStream(file));
                    // input.read(buffer,0,buffer.length);
                    // input.close();
                    String timeStamp = new java.text.SimpleDateFormat("HHmmss").format(new Date());
                    int n=filesize/CHUNKSIZE;
                    if(filesize%CHUNKSIZE!=0) n++;
                    int left = filesize;
                    for(int j=0; j<n; j++){
                        int tmpsize = (left>=CHUNKSIZE?CHUNKSIZE:left);
                        byte[] tmp = new byte[tmpsize];
                        raf.seek(CHUNKSIZE*j);
                        raf.read(tmp);
                        // System.arraycopy(tmp, CHUNKSIZE*j, tmp, 0, tmpsize);
                        fi.uploadFile(str[0], tmp, timeStamp, j);
                        left-=tmpsize;
                    }
                    File file2= new File(cacheDir, str[0]+"#"+timeStamp);
                    if(notUse.containsKey(str[0])){
                        File ff = new File(cacheDir, notUse.get(str[0]));
                        cacheUsed -= ff.length();
                        System.err.println("delete 8 "+file.getName());
                        ff.delete();
                    }
                    file.renameTo(file2);
                    cf.name=str[0]+"#"+timeStamp;
                    notUse.put(str[0], str[0]+"#"+timeStamp);
                } catch(Exception ex){
                    System.err.println("Error close write file!");
                }
            }
            return 0;
		}

		public long write( int fd, byte[] buf ) {
			System.err.printf("Write!\n");
            fd = fd-OFFSET;
            cacheFile cf = fdTable.get(fd);   
            if (cf.closed) {
                return Errors.EBADF;
            }
            if (!cf.canWrite) {
                return Errors.EBADF;
            }
            cacheUsed+=buf.length;
            if(!checkCache()){
                cacheUsed-=buf.length;
                return Errors.ENOMEM;
            }
            FileChannel fc = (cf.raf).getChannel();
            try{
                int r = fc.write(ByteBuffer.wrap(buf));
                if (r == -1) return 0;
                return r;
            } catch (Exception e) {
            	e.printStackTrace();
            	return Errors.EINVAL;
            }
		}

		public long read( int fd, byte[] buf ) {
			System.err.printf("Read!\n");
            fd = fd-OFFSET;
            cacheFile cf = fdTable.get(fd);
            if (cf.closed) {
                return Errors.EBADF;
            }
            if (cf.isDir) {
                return Errors.EISDIR;
            } else {
                FileChannel fc = (cf.raf).getChannel();
                try {
                    int r = fc.read(ByteBuffer.wrap(buf));
                    if (r == -1) return 0;
                    else return r;
                } catch (IOException e) {return Errors.EINVAL;}
            }
		}

		public long lseek( int fd, long pos, LseekOption o ) {
			System.err.printf("lseek!\n");
            fd = fd-OFFSET;
            cacheFile cf = fdTable.get(fd);
            
            if (cf.closed) {
                return Errors.EBADF;
            }
            try {
                RandomAccessFile f = cf.raf;
                long curPos = f.getFilePointer();
                long length = f.length();
                switch (o) {
                    case FROM_CURRENT:
                        if ((pos+curPos) < 0) {
                            return Errors.EINVAL;
                        }
                        f.seek(pos+curPos);
                        return (pos+curPos);
                    case FROM_END:
                        if ((pos+length) < 0) {
                            return Errors.EINVAL;
                        }
                        f.seek(pos+length);
                        return (pos+length);
                    case FROM_START:
                        if (pos < 0) {
                            return Errors.EINVAL;
                        }
                        f.seek(pos);
                        return (pos);
                    default:
                        break;
                }
            } catch (IOException e){}
            return Errors.ENOSYS;
		}

		public int unlink( String path ) {
			System.err.printf("Unlink!\n");
            String name = "//"+serverIP+":"+serverPort+"/Server";
            String[] serverState = {"false", "false", "000000", "false", "0"};
            path=parsePath(path);
            try{
                FileInterface fi = (FileInterface) Naming.lookup(name);
                serverState=fi.checkFile(path);
            } catch(Exception e){
                e.printStackTrace();
            }
            if(serverState[3].equals("false")) return Errors.EPERM;
            if (serverState[0].equals("false")) {
                return Errors.ENOENT;
            }
            if (serverState[1].equals("true")) {
                return Errors.EISDIR;
            }
            try{
                FileInterface fi = (FileInterface) Naming.lookup(name);
                if(!fi.deleteFile(path)) return -1;
            } catch(Exception e){
                e.printStackTrace();
            }
            return 0;
		}

		public void clientdone() {
			return;
		}

	}
	
	private static class FileHandlingFactory implements FileHandlingMaking {
        public FileHandling newclient() {
            return new FileHandler();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            throw new IllegalArgumentException("Number of expected args are 4");
        }
        serverIP = String.valueOf(args[0]);
        serverPort = Integer.parseInt(args[1]);
        cacheDir = String.valueOf(args[2]);
        cacheSize = Integer.parseInt(args[3]);
        try{
            String name = "//"+serverIP+":"+serverPort+"/Server";
            fi = (FileInterface) Naming.lookup(name);
        }catch(Exception ex){
            System.err.println("Error connecting server");
        }
        reading = new HashMap<String, Integer>();
        cacheQueue = new LinkedList<String>();
        notUse = new HashMap<String, String>();
        cacheUsed=0;
        System.err.println("cacheSize: "+cacheSize);
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}

