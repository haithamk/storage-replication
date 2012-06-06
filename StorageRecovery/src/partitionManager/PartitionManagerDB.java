package partitionManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionManagerDB {

	class Table{
		
		//TODO synchornize accesses to the table
		String table_name;
		Map<String, String> table;
		Set<String> authorized_users;
		
		public Table(){
			table = new HashMap<String, String>();
			authorized_users = new HashSet<String>();
		}
		
	}
	
	
	public int port;
	
}
