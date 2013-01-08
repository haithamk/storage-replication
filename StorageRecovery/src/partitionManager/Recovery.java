package partitionManager;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Recovery {

	
	//http://stackoverflow.com/questions/2332537/producer-consumer-threads-using-a-queue
	//http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html
	
	class TableInfo{
		String table_name;
		String dn_ip;
		int dn_port;
	}
	
	LinkedList<TableInfo> tables_info;
	PartitionManagerDB pm_db;
	
	
	
	public static boolean recover(PartitionManagerDB pm_db){
		boolean result = false;
		
		Recovery recovery = new Recovery();
		recovery.pm_db = pm_db;
		
		return result;
	}
	
	private Recovery(){
		tables_info = new LinkedList<Recovery.TableInfo>();
	}
	
	private void getDNList(){
		
	}
	
	
	private void initQueue(){
		
	}
	
	private boolean startRecovery(){
		
		try {
			final ExecutorService executers = Executors.newFixedThreadPool(100);
			while(!tables_info.isEmpty()){
				TableInfo table_info = tables_info.poll();
				TableBuilder table_builder = new TableBuilder(pm_db, table_info);
				executers.submit(table_builder);
			}
			
			executers.shutdown();
			executers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			return true;
		} catch (InterruptedException e) {
			//TODO fatal error. suspend work
			e.printStackTrace();
			return false;
		}
	}
	
}
