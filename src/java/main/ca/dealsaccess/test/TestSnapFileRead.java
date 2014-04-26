package ca.dealsaccess.test;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TestSnapFileRead {

	//private static final File snapDir = new File("/home/hadoop-user/pseudo_zookeeper/server001/data/version-2");
	private static final File snapDir = new File("/home/hadoop-user/workspace/zookeeper/data/version-2");
	
	private static final Logger LOG = LoggerFactory.getLogger(TestSnapFileRead.class);
	
	public final static int SNAP_MAGIC
    = ByteBuffer.wrap("ZKSN".getBytes()).getInt();
	
	public static void main(String[] args) throws IOException {
		File lastSnap = null;
		
		File[] files = snapDir.listFiles();
		for(File f : files) {
			long zxid = getZxidFromName(f.getName(), "snapshot");
			System.out.println("filname: "+f.getName());
			System.out.println("zxid: "+zxid);
		}
		
		
		
		
		List<File> snapList = findNValidSnapshots(100);
		boolean foundValid = false;
		for(File snap : snapList) {
			
			InputStream snapIS = null;
			CheckedInputStream crcIn = null;
			
			try {
				//snap = new File(snapDir+"/snapshot.500000005");
	            LOG.info("Reading snapshot " + snap);
	            snapIS = new BufferedInputStream(new FileInputStream(snap));
	            crcIn = new CheckedInputStream(snapIS, new Adler32());
	            InputArchive ia = BinaryInputArchive.getArchive(crcIn);
	            
	            /*FileHeader.deserialize()*/
	            int magic = ia.readInt("magic");
	            int version = ia.readInt("version");
	            long dbid = ia.readLong("dbid");
	            System.out.println("Magic: "+magic);
	            System.out.println("Version: "+version);
	            System.out.println("Dbid: "+dbid);
	            
	            /*SerializeUtils.deserializeSnapshot()*/
	            int count = ia.readInt("count");
	            System.out.println("Count: "+count);
	            int count2 = count;
	            while(count > 0) {
	            	int cnt = count2-count;
	            	long id = ia.readLong("id");
	                int to = ia.readInt("timeout");
	                System.out.println(cnt+": id: "+id);
	                System.out.println(cnt+": timeout: "+to);
	            	count--;
	            }
	            
	            /*DataTree.deserialize()*/
	            int i = ia.readInt("map");
	            System.out.println("map: "+i);
	            int i2 = i;
	            while(i > 0) {
	            	int cnt2 = i2-i;
	            	Long val = ia.readLong("long");
	            	System.out.println("i="+cnt2+": long: "+val);
	            	/*ia.startVector()*/
	            	int aclLen = ia.readInt("aclLen");
	            	System.out.println("i="+cnt2+": aclLen: "+aclLen);
	            	while(aclLen > 0) {
	            		/*ACL.deserialize()*/
	            		int perms=ia.readInt("perms");
	            		System.out.println("i="+cnt2+": acl="+"perms: "+perms);
	            		/*org.apache.zookeeper.data.Id.deserialize()*/
	            		String scheme=ia.readString("scheme");
	            	    String id=ia.readString("id");
	            	    System.out.println("i="+cnt2+": acl="+"scheme: "+scheme);
	            	    System.out.println("i="+cnt2+": acl="+"id: "+id);
	            		aclLen--;
	            	}
	            	i--;
	            }
	            
	            String path = ia.readString("path");
	            int pathCnt = 0;
	            System.out.println();
	            System.out.println(pathCnt+": path: "+path);
	            while (!"/".equals(path)) {
	            
	            
	            
	            
	            
	            
		            /*DataNode.deserialize()*/
		            byte[] data = ia.readBuffer("data");
		            long acl = ia.readLong("acl");
		            String dataStr = new String(data, "UTF-8");
		            System.out.println(pathCnt+": dataStr: "+dataStr);
		            System.out.println(pathCnt+": acl: "+acl);
		            
		            /*StatPersisted.deserialize()*/
		            long czxid=ia.readLong("czxid");
		            long mzxid=ia.readLong("mzxid");
		            long ctime=ia.readLong("ctime");
		            long mtime=ia.readLong("mtime");
		            int statVersion=ia.readInt("version");
		            int cversion=ia.readInt("cversion");
		            int aversion=ia.readInt("aversion");
		            long ephemeralOwner=ia.readLong("ephemeralOwner");
		            long pzxid=ia.readLong("pzxid");
		            System.out.println(pathCnt+": czxid: "+czxid);
		            System.out.println(pathCnt+": mzxid: "+mzxid);
		            System.out.println(pathCnt+": ctime: "+ctime);
		            System.out.println(pathCnt+": mtime: "+mtime);
		            System.out.println(pathCnt+": statVersion: "+statVersion);
		            System.out.println(pathCnt+": cversion: "+cversion);
		            System.out.println(pathCnt+": aversion: "+aversion);
		            System.out.println(pathCnt+": ephemeralOwner: "+ephemeralOwner);
		            System.out.println(pathCnt+": pzxid: "+pzxid);
	            
		            path = ia.readString("path");
		            pathCnt++;
		            System.out.println();
		            System.out.println(pathCnt+": path: "+path);
	            }
	            
	            
	            
	            long checkSum = crcIn.getChecksum().getValue();
	            long val = ia.readLong("val");
	            if (val != checkSum) {
	                throw new IOException("CRC corruption in snapshot :  " + snap);
	            }
	            foundValid = true;
	            lastSnap = new File(snap.getAbsolutePath());
	            break;
	            
	            
	        } catch(IOException e) {
	            LOG.warn("problem reading snap file " + snap, e);
	        } finally {
	            if (snapIS != null) 
	                snapIS.close();
	            if (crcIn != null) 
	                crcIn.close();
	        } 
			
			
			
			
			
		}
		
		
		
		
		
		if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        long lastProcessedZxid = getZxidFromName(lastSnap.getName(), "snapshot");
        System.out.println("lastProcessedZxid: "+lastProcessedZxid);
        
	}
	
	
	/**
     * Compare file file names of form "prefix.version". Sort order result
     * returned in order of version.
     */
    private static class DataDirFileComparator
        implements Comparator<File>, Serializable
    {
        private static final long serialVersionUID = -2648639884525140318L;

        private String prefix;
        private boolean ascending;
        public DataDirFileComparator(String prefix, boolean ascending) {
            this.prefix = prefix;
            this.ascending = ascending;
        }

        public int compare(File o1, File o2) {
            long z1 = getZxidFromName(o1.getName(), prefix);
            long z2 = getZxidFromName(o2.getName(), prefix);
            int result = z1 < z2 ? -1 : (z1 > z2 ? 1 : 0);
            return ascending ? result : -result;
        }
    }
    
    /**
     * Sort the list of files. Recency as determined by the version component
     * of the file name.
     *
     * @param files array of files
     * @param prefix files not matching this prefix are assumed to have a
     * version = -1)
     * @param ascending true sorted in ascending order, false results in
     * descending order
     * @return sorted input files
     */
    public static List<File> sortDataDir(File[] files, String prefix, boolean ascending)
    {
        if(files==null)
            return new ArrayList<File>(0);
        List<File> filelist = Arrays.asList(files);
        Collections.sort(filelist, new DataDirFileComparator(prefix, ascending));
        return filelist;
    }
    
    
    public static long getZxidFromName(String name, String prefix) {
        long zxid = -1;
        String nameParts[] = name.split("\\.");
        if (nameParts.length == 2 && nameParts[0].equals(prefix)) {
            try {
                zxid = Long.parseLong(nameParts[1], 16);
            } catch (NumberFormatException e) {
            }
        }
        return zxid;
    }
    
    public static boolean isValidSnapshot(File f) throws IOException {
        if (f==null || getZxidFromName(f.getName(), "snapshot") == -1)
            return false;

        // Check for a valid snapshot
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        try {
            // including the header and the last / bytes
            // the snapshot should be at least 10 bytes
            if (raf.length() < 10) {
                return false;
            }
            raf.seek(raf.length() - 5);
            byte bytes[] = new byte[5];
            int readlen = 0;
            int l;
            while(readlen < 5 &&
                  (l = raf.read(bytes, readlen, bytes.length - readlen)) >= 0) {
                readlen += l;
            }
            if (readlen != bytes.length) {
                LOG.info("Invalid snapshot " + f
                        + " too short, len = " + readlen);
                return false;
            }
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            int len = bb.getInt();
            byte b = bb.get();
            if (len != 1 || b != '/') {
                LOG.info("Invalid snapshot " + f + " len = " + len
                        + " byte = " + (b & 0xff));
                return false;
            }
        } finally {
            raf.close();
        }

        return true;
    }
    
    
    private static List<File> findNValidSnapshots(int n) throws IOException {
        List<File> files = sortDataDir(snapDir.listFiles(),"snapshot", false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                if (isValidSnapshot(f)) {
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }
	
}
