package partitionManager;

import partitionManager.Recovery.TableInfo;

public class TableBuilder implements Runnable {

	TableInfo table_info;
	PartitionManagerDB pm_db;
	
	public TableBuilder(PartitionManagerDB pm_db, TableInfo table_info){
		this.pm_db = pm_db;
		this.table_info = table_info;
	}
	
	@Override
	public void run() {
		//TODO 1. retrieve XML from data Node
		//2. construct table
		//3. save to db
	}

}
