package ca.dealsaccess.test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.server.DataNode;

public class Util {
	
	static boolean end = false;
	
	final static String filename = "data/test.out";
	
	static FileWriter fw = null;
	
	final public static void PrintNodes(ConcurrentHashMap<String, DataNode> nodes) throws IOException {
		
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		for(String key : nodes.keySet()) {
			DataNode node = nodes.get(key);
			sb.append("'").append(key).append("': {'data': \"");
			sb.append(new String(node.getData(), "UTF-8")).append("\", 'children': [");
			Set<String> children = node.getChildren();
			if(children == null) {
				children = new HashSet<String>(8);
			}
			for(String chr: children) {
				sb.append(chr).append(",");
			}
			sb.append("]}, ");
		}
		sb.append("}\n");
		
		if(fw == null) {
			fw = new FileWriter(filename);
		}
		fw.write(sb.toString());
		if(end) {
			fw.close();
		}
	}





	public static boolean isEnd() {
		return end;
	}





	public static void setEnd(boolean e) {
		end = e;
	}
	
	
}
