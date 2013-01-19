package partitionManager;

import java.net.Socket;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import messages.RecoverPMMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Recovery {

	
	//http://stackoverflow.com/questions/2332537/producer-consumer-threads-using-a-queue
	//http://docs.oracle.com/javase/6/docs/api/java/util/concurrent/ExecutorService.html
	
	class TableInfo{
		
		public TableInfo(String table_name, String dn_ip, int dn_port){
			this.table_name = table_name;
			this.dn_ip = dn_ip;
			this.dn_port = dn_port;
		}
		
		public String table_name;
		public String dn_ip;
		public int dn_port;
	}
	
	LinkedList<TableInfo> tables_info;
	PartitionManagerDB pm_db;
	RecoverPMMessage recover_msg;
	static final Logger logger = LoggerFactory.getLogger(Recovery.class);
	
	
	
	public static boolean recover(PartitionManagerDB pm_db, RecoverPMMessage recover_msg){
		boolean result = false;
		Recovery recovery = new Recovery(pm_db, recover_msg);
		
		recovery.initDNs();
		recovery.startRecovery();
		
		return result;
	}
	
	private Recovery(PartitionManagerDB pm_db, RecoverPMMessage recover_msg){
		tables_info = new LinkedList<Recovery.TableInfo>();
		this.pm_db = pm_db;
		this.recover_msg = recover_msg;
	}
	
	
	
	private void initDNs(){
		for(int i = 0; i < recover_msg.table_names.size(); i++){
			String table_name = recover_msg.table_names.get(i);
			String[] replicas = recover_msg.replicas.get(i);
			
			pm_db.replicas.put(table_name, replicas);
			
			String ip = replicas[0].split(":")[0];
        	int port = Integer.parseInt(replicas[0].split(":")[1]);
        	
			TableInfo table_info = new TableInfo(table_name, ip, port);
			tables_info.add(table_info);
		}
		
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
