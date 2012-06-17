package partitionManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class PartitionManagerDB {

	
	
	
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
		
		table.lockRow(key);
		table.store(key, value);
		table.unlockRow(key);
		
		return true;		
	}
	
	protected String read(String table_name, String key){
		Table table = tables.get(table_name);
		if(table == null){
			return null;
		}
		
		table.lockRow(key);
		String result =  table.read(key); 
		table.unlockRow(key);
		
		return result;
	}
	
	protected void delete(String table_name, String key){
		Table table = tables.get(table_name);
		if(table == null){
			return;
		}
		
		table.lockRow(key);
		table.delete(key); 
		table.unlockRow(key);
	}
	
	
	
	
}
