package orchestrator;

public class OrchestratorDB {
	public int port;
	public String active_pm_address;
	
	
	
	/**
	 * Atomically return the active PM address
	 */
	public String getActivePM(){
		String current_pm;
		synchronized(active_pm_address){
			current_pm = active_pm_address;
		}
		return current_pm;
	}
	
	/**
	 * Atomically set the active PM address
	 */
	public void setActivePM(String new_pm){
		synchronized(active_pm_address){
			active_pm_address = new_pm;
		}
	}
	
}
