package ca.dealsaccess.test;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
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
import java.util.zip.Checksum;

import org.apache.jute.test.BinaryInputArchive;
import org.apache.jute.test.InputArchive;
import org.apache.jute.test.Record;
import org.apache.zookeeper.server.persistence.test.FileHeader;
import org.apache.zookeeper.test.ZooDefs.OpCode;
import org.apache.zookeeper.txn.test.CreateSessionTxn;
import org.apache.zookeeper.txn.test.CreateTxn;
import org.apache.zookeeper.txn.test.CreateTxnV0;
import org.apache.zookeeper.txn.test.DeleteTxn;
import org.apache.zookeeper.txn.test.ErrorTxn;
import org.apache.zookeeper.txn.test.MultiTxn;
import org.apache.zookeeper.txn.test.SetACLTxn;
import org.apache.zookeeper.txn.test.SetDataTxn;
import org.apache.zookeeper.txn.test.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class TestTxnLogFileRead {
private static final File logDir = new File("/home/hadoop-user/workspace/zookeeper/logs/version-2");
	
	private static final Logger LOG = LoggerFactory.getLogger(TestTxnLogFileRead.class);
	
	private final static long lastProcessedZxid = 34359738369L;
	//21474836481
	//25769803777
	//21474836485L
	public final static int TXNLOG_MAGIC =
	        ByteBuffer.wrap("ZKLG".getBytes()).getInt();
	
	public static void main(String[] args) throws IOException {
		
		File[] files = logDir.listFiles();
		for(File f : files) {
			long zxid = getZxidFromName(f.getName(), "log");
			System.out.println("filname: "+f.getName());
			System.out.println("zxid: "+zxid);
		}
		
		FileTxnIterator itr = new FileTxnIterator(logDir, (lastProcessedZxid+1), true);
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
	
	
	/**
     * a class that keeps track of the position 
     * in the input stream. The position points to offset
     * that has been consumed by the applications. It can 
     * wrap buffered input streams to provide the right offset 
     * for the application.
     */
    static class PositionInputStream extends FilterInputStream {
        long position;
        protected PositionInputStream(InputStream in) {
            super(in);
            position = 0;
        }
        
        @Override
        public int read() throws IOException {
            int rc = super.read();
            if (rc > -1) {
                position++;
            }
            return rc;
        }

        public int read(byte[] b) throws IOException {
            int rc = super.read(b);
            if (rc > 0) {
                position += rc;
            }
            return rc;            
        }
        
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int rc = super.read(b, off, len);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        
        @Override
        public long skip(long n) throws IOException {
            long rc = super.skip(n);
            if (rc > 0) {
                position += rc;
            }
            return rc;
        }
        public long getPosition() {
            return position;
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void mark(int readLimit) {
            throw new UnsupportedOperationException("mark");
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("reset");
        }
    }
	
    
    /**
     * Find the log file that starts at, or just before, the snapshot. Return
     * this and all subsequent logs. Results are ordered by zxid of file,
     * ascending order.
     * @param logDirList array of files
     * @param snapshotZxid return files at, or before this zxid
     * @return
     */
    public static File[] getLogFiles(File[] logDirList,long snapshotZxid) {
        List<File> files = sortDataDir(logDirList, "log", true);
        long logZxid = 0;
        // Find the log file that starts before or at the same time as the
        // zxid of the snapshot
        for (File f : files) {
            long fzxid = getZxidFromName(f.getName(), "log");
            if (fzxid > snapshotZxid) {
                continue;
            }
            // the files
            // are sorted with zxid's
            if (fzxid > logZxid) {
                logZxid = fzxid;
            }
        }
        List<File> v=new ArrayList<File>(5);
        for (File f : files) {
            long fzxid = getZxidFromName(f.getName(), "log");
            if (fzxid < logZxid) {
                continue;
            }
            v.add(f);
        }
        return v.toArray(new File[0]);

    }
    
	/**
     * this class implements the txnlog iterator interface
     * which is used for reading the transaction logs
     */
    public static class FileTxnIterator {
        File logDir;
        long zxid;
        TxnHeader hdr;
        Record record;
        File logFile;
        InputArchive ia;
        static final String CRC_ERROR="CRC check failed";
       
        PositionInputStream inputStream=null;
        //stored files is the list of files greater than
        //the zxid we are looking for.
        private ArrayList<File> storedFiles;

        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @param fastForward   true if the iterator should be fast forwarded to
         *        point to the txn of a given zxid, else the iterator will
         *        point to the starting txn of a txnlog that may contain txn of
         *        a given zxid
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid, boolean fastForward)
                throws IOException {
            this.logDir = logDir;
            this.zxid = zxid;
            init();

            if (fastForward && hdr != null) {
            	System.out.println("In if (fastForward && hdr != null) {");
            	while (hdr.getZxid() < zxid) {
                	System.out.println("hdr zxid:"+hdr.getZxid());
                	if (!next())
                	    break;
                }
            }
        }
        
        /**
         * create an iterator over a transaction database directory
         * @param logDir the transaction database directory
         * @param zxid the zxid to start reading from
         * @throws IOException
         */
        public FileTxnIterator(File logDir, long zxid) throws IOException {
            this(logDir, zxid, true);
        }

        /**
         * initialize to the zxid specified
         * this is inclusive of the zxid
         * @throws IOException
         */
        void init() throws IOException {
            storedFiles = new ArrayList<File>();
            List<File> files = sortDataDir(getLogFiles(logDir.listFiles(), 0), "log", false);
            for (File f: files) {
                if (getZxidFromName(f.getName(), "log") >= zxid) {
                    storedFiles.add(f);
                }
                // add the last logfile that is less than the zxid
                else if (getZxidFromName(f.getName(), "log") < zxid) {
                    storedFiles.add(f);
                    break;
                }
            }
            System.out.print("storedFiles: [");
            int i = 0;
            for(File f1: storedFiles) {
            	if(i == 0) {
            		System.out.print(f1.getName()+":"+getZxidFromName(f1.getName(), "log"));
            		
            	} else {
            		System.out.print(","+f1.getName()+":"+getZxidFromName(f1.getName(), "log"));
            	}
            	i++;
            }
            System.out.println("]");
            goToNextLog();
            
            
            
            
            
            
            if (!next())
                return;
        }
        
        /**
         * Return total storage size of txnlog that will return by this iterator.
         */
        public long getStorageSize() {
            long sum = 0;
            for (File f : storedFiles) {
                sum += f.length();
            }
            return sum;
        }

        /**
         * go to the next logfile
         * @return true if there is one and false if there is no
         * new file to be read
         * @throws IOException
         */
        private boolean goToNextLog() throws IOException {
        	
            if (storedFiles.size() > 0) {
            	System.out.println("Start goTo nextLog "+storedFiles.get(storedFiles.size()-1).getName());
                this.logFile = storedFiles.remove(storedFiles.size()-1);
                ia = createInputArchive(this.logFile);
                return true;
            }
            System.out.println("No more txnLog");
            return false;
        }

        /**
         * read the header from the inputarchive
         * @param ia the inputarchive to be read from
         * @param is the inputstream
         * @throws IOException
         */
        protected void inStreamCreated(InputArchive ia, InputStream is)
            throws IOException{
            FileHeader header= new FileHeader();
            header.deserialize(ia, "fileheader");
            if (header.getMagic() != TXNLOG_MAGIC) {
                throw new IOException("Transaction log: " + this.logFile + " has invalid magic number " 
                        + header.getMagic()
                        + " != " + TXNLOG_MAGIC);
            }
        }

        /**
         * Invoked to indicate that the input stream has been created.
         * @param ia input archive
         * @param is file input stream associated with the input archive.
         * @throws IOException
         **/
        protected InputArchive createInputArchive(File logFile) throws IOException {
            if(inputStream==null){
                inputStream= new PositionInputStream(new BufferedInputStream(new FileInputStream(logFile)));
                LOG.debug("Created new input stream " + logFile);
                ia  = BinaryInputArchive.getArchive(inputStream);
                inStreamCreated(ia,inputStream);
                LOG.debug("Created new input archive " + logFile);
            }
            return ia;
        }

        /**
         * create a checksum algorithm
         * @return the checksum algorithm
         */
        protected Checksum makeChecksumAlgorithm(){
            return new Adler32();
        }

        /**
         * the iterator that moves to the next transaction
         * @return true if there is more transactions to be read
         * false if not.
         */
        public boolean next() throws IOException {
            if (ia == null) {
                return false;
            }
            try {
                long crcValue = ia.readLong("crcvalue");
                System.out.println("crcvalue: "+crcValue);
                byte[] bytes = readTxnBytes(ia);
                
                // Since we preallocate, we define EOF to be an
                if (bytes == null || bytes.length==0) {
                    throw new EOFException("Failed to read " + logFile);
                }
                // EOF or corrupted record
                // validate CRC
                Checksum crc = makeChecksumAlgorithm();
                crc.update(bytes, 0, bytes.length);
                if (crcValue != crc.getValue())
                    throw new IOException(CRC_ERROR);
                if (bytes == null || bytes.length == 0)
                    return false;
                hdr = new TxnHeader();
                record = deserializeTxn(bytes, hdr);
                
                
                
                
                
            } catch (EOFException e) {
            	System.out.println("EOF excepton " + e);
                LOG.debug("EOF excepton " + e);
                inputStream.close();
                inputStream = null;
                ia = null;
                hdr = null;
                // this means that the file has ended
                // we should go to the next file
                if (!goToNextLog()) {
                    return false;
                }
                // if we went to the next log file, we should call next() again
                return next();
            } catch (IOException e) {
                inputStream.close();
                throw e;
            }
            return true;
        }

        /**
         * reutrn the current header
         * @return the current header that
         * is read
         */
        public TxnHeader getHeader() {
            return hdr;
        }

        /**
         * return the current transaction
         * @return the current transaction
         * that is read
         */
        public Record getTxn() {
            return record;
        }

        /**
         * close the iterator
         * and release the resources.
         */
        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
        }
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
    
    public static byte[] readTxnBytes(InputArchive ia) throws IOException {
        try{
            byte[] bytes = ia.readBuffer("txtEntry");
            
            // Since we preallocate, we define EOF to be an
            // empty transaction
            if (bytes.length == 0)
                return bytes;
            if (ia.readByte("EOF") != 'B') {
                LOG.error("Last transaction was partial.");
                return null;
            }
            return bytes;
        }catch(EOFException e){}
        return null;
    }
    
    public static Record deserializeTxn(byte txnBytes[], TxnHeader hdr)
            throws IOException {
    	/*SerializeUtils.deserializeTxn()*/
        final ByteArrayInputStream bais = new ByteArrayInputStream(txnBytes);
        InputArchive ia = BinaryInputArchive.getArchive(bais);
        
        /*TxnHeader.deserialize()*/
        hdr.deserialize(ia, "hdr");
        bais.mark(bais.available());
        Record txn = null;
        switch (hdr.getType()) {
        case OpCode.createSession:
            // This isn't really an error txn; it just has the same
            // format. The error represents the timeout
            txn = new CreateSessionTxn();
            break;
        case OpCode.closeSession:
        	System.out.println("Record type: closeSession");
            return null;
        case OpCode.create:
        case OpCode.create2:
            txn = new CreateTxn();
            break;
        case OpCode.delete:
            txn = new DeleteTxn();
            break;
        case OpCode.reconfig:
        case OpCode.setData:
            txn = new SetDataTxn();
            break;
        case OpCode.setACL:
            txn = new SetACLTxn();
            break;
        case OpCode.error:
            txn = new ErrorTxn();
            break;
        case OpCode.multi:
            txn = new MultiTxn();
            break;
        default:
            throw new IOException("Unsupported Txn with type=%d" + hdr.getType());
        }
        if (txn != null) {
            try {
                txn.deserialize(ia, "txn");
            } catch(EOFException e) {
                // perhaps this is a V0 Create
                if (hdr.getType() == OpCode.create) {
                    CreateTxn create = (CreateTxn)txn;
                    bais.reset();
                    CreateTxnV0 createv0 = new CreateTxnV0();
                    createv0.deserialize(ia, "txn");
                    // cool now make it V1. a -1 parentCVersion will
                    // trigger fixup processing in processTxn
                    create.setPath(createv0.getPath());
                    create.setData(createv0.getData());
                    create.setAcl(createv0.getAcl());
                    create.setEphemeral(createv0.getEphemeral());
                    create.setParentCVersion(-1);
                } else {
                    throw e;
                }
            }
        }
        return txn;
    }
}
