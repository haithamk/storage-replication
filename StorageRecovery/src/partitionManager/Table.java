package partitionManager;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Table{
	
	class Data{
		String value;
		ReentrantReadWriteLock lock;
		
		public Data(){
			value = null;
			lock = new ReentrantReadWriteLock();
		}
		
		public Data(String value){
			this.value = value;
		}
	}
	
	
	private static final Logger logger = LoggerFactory.getLogger(Table.class);
	private String table_name;
	private Hashtable<String, Data> table;
	private Set<String> authorized_users;
	private ReentrantReadWriteLock lock;
	
	
	
	public Table(String table_name){
		this.table_name = table_name;
		table = new Hashtable<String, Data>();
		authorized_users = new HashSet<String>();
		lock = new ReentrantReadWriteLock();
	}
	
	
	
	public ReentrantReadWriteLock getRowLock(String key){
		//Acquiring read lock to retrieve the data
		logger.debug("Acquirung lock for key: {} from table: {}", key, table_name);
		lock.readLock().lock();
		Data data = table.get(key);
		if(data == null){
			logger.debug("Data object doesn't exist. Acquirung write lock");
			//Data doesn't exists. Abandon read lock and acquire write lock to
			//create and add data to table
			lock.readLock().unlock();
			lock.writeLock().lock();
			//Check if someone else added the data since the last check
			data = table.get(key);
			if(data == null){
				logger.debug("Data object still doesn't exist. creating new one");
				//Data still not added. Add it
				//Nobody can do this code concurrently since we acquired the write lock
				data = new Data();
				table.put(key, data);
			}		
			//Abandon the write lock
			lock.readLock().lock();
			lock.writeLock().unlock();
		}
		lock.readLock().unlock();
		
		return data.lock;
	}
	
	
	
	
	public void store(String key, String value){
		Data data = table.get(key);
		if(data == null){
			//This case SHOULDN'T happen ever. since the thread adding a new value should lock the row first
			logger.debug("Severe Error occurred!! attemping to store a value to non-existing row");
			return;
		}
		data.value = value;
	}
	
	
	public String read(String key){
		Data data = table.get(key);
		if(data == null){
			//This case SHOULDN'T happen ever. since the thread adding a new value should lock the row first
			logger.debug("Severe Error occurred!! attemping to store a value to non-existing row");
			return null;
		}
		
		return data.value;
	}
	
	
	public void delete(String key){
		table.remove(key);
	}
	
}