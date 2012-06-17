package partitionManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Table{
	
	
	class Data{
		String value;
		Lock lock = new ReentrantLock();;
		
		public Data(){
			value = "";
			//lock
		}
		
		public Data(String value){
			this.value = value;
		}
	}
	
	//TODO synchronize accesses to the table
	private String table_name;
	private Hashtable<String, Data> table;
	private Set<String> authorized_users;
	
	public Table(String table_name){
		this.table_name = table_name;
		table = new Hashtable<String, Data>();
		authorized_users = new HashSet<String>();
	}
	
	public void lockRow(String key){
		//Change to read write lock
		Data data = table.get(key);
		if(data == null){
			data = new Data();
			table.put(key, data);
		}
		
		data.lock.lock();
	}
	
	
	public void unlockRow(String key){
		Data data = table.get(key);
		if(data != null){
			data.lock.unlock();
		}
	}
	
	
	public void store(String key, String value){
		table.put(key, new Data(value));
	}
	
	public String read(String key){
		Data data = table.get(key);
		if(data != null){
			return data.value;
		}else{
			return null;
		}
	}
	
	
	public void delete(String key){
		table.remove(key);
	}
	
}