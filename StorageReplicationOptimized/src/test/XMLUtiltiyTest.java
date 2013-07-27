package test;

import utilities.XMLUtility;

public class XMLUtiltiyTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	
		String file_path = "F:/Test/test.xml";
		XMLUtility.createFile(file_path);
		XMLUtility.addStoreOperation(file_path, "A", "Hello");
		XMLUtility.addDeleteOperation(file_path, "A");
	}

}
