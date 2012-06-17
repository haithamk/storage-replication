package partitionManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class PartitionManagerDB {

	class Table{
		//TODO synchronize accesses to the table
		String table_name;
		Map<String, String> table;
		Set<String> authorized_users;
		
		public Table(String table_name){
			this.table_name = table_name;
			table = new HashMap<String, String>();
			authorized_users = new HashSet<String>();
		}
	}
	
	
	private Hashtable<String, Table> tables;
	public int port;
	
	
	protected void createTable(String table_name){
		Table table = new Table(table_name);
		tables.put(table_name, table);
	}
	
	
	protected void dropTable(String table_name){
		tables.remove(table_name);
	}
	
	
	protected boolean store(String table_name, String key, String value){
		Table table = tables.get(table_name);
		if(table == null){
			return false;
		}
		
		table.table.put(key, value);
		return true;		
	}
	
	protected String read(String table_name, String key){
		Table table = tables.get(table_name);
		if(table == null){
			return null;
		}
		
		return table.table.get(key); 
	}
	
	protected void delete(String table_name, String key){
		Table table = tables.get(table_name);
		if(table == null){
			return;
		}
		
		table.table.get(key); 
	}
	
	
	
	
}
