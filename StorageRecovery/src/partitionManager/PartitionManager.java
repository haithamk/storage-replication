package partitionManager;

import java.util.HashMap;
import java.util.LinkedList;


import shared.NodeAddress;
import shared.TableInfo;

public class PartitionManager {
	private HashMap<String, Table> tables;
	
	public PartitionManager(String config_file) {
		// TODO Auto-generated constructor stub
	}
	public boolean createTable(String user_name ,String table_name){
		return false;}
	public boolean deleteTable(String user_name ,String table_name){
		return false;}
	public boolean putObject(String user_name ,String table_name,Object object){
		return false;}
	public Object getObject(String user_name ,String table_name , String key){
		return key;}
	public boolean deleteObject(String user_name ,String table_name ,String key){
		return false;}

	public void recover(LinkedList<TableInfo> tables_list){}
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	
}
