import java.util.HashMap;
import java.util.LinkedList;
import java.awt.*;
import java.awt.event.*;
import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;
import javax.swing.*;


public class Phonebook {
	private static HashMap<String ,String> users;
	private static Myclass1 users_array[];
	static Integer index;
	public static void main(String[] args){
		try {
			UIManager.setLookAndFeel( "com.sun.java.swing.plaf.nimbus.NimbusLookAndFeel");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		users=new HashMap<String ,String>();
		users_array=new Myclass1[20];
		index=0;
		new Firstframe();
	}
	public HashMap<String ,String> getUsers(){
		return Phonebook.users;
	}
    public static void addClient(String user_name ,String password){
        users.put(user_name, password);
        //users_list.add(user_name);   
    }
    public static String findClient(String user_name){
        return users.get(user_name);
    }
    public static void addNewUser(String user_name){
    	users_array[index]=new Myclass1(user_name);
    	++index;
    }
    public static void addNewContact(String user_name ,String contact_name ,Integer contact_number){
    	int i;
    	for(i=0;i<index;++i){
    		if(user_name.equals(users_array[i].user_name)){
    			index=i;
    			break;
    		}
    	}
    	users_array[index].addContact(contact_name, contact_number);
    }
    public static void modifyContact(String user_name ,String contact_name ,Integer contact_number){
    	int i;
    	for(i=0;i<index;++i){
    		if(user_name.equals(users_array[i].user_name)){
    			index=i;
    			break;
    		}
    	}
    	users_array[index].modifyContact(contact_name, contact_number);
    }
    public static Integer searchContact(String user_name ,String contact_name){
    	int i;
    	for(i=0;i<index;++i){
    		if(user_name.equals(users_array[i].user_name)){
    			index=i;
    			break;
    		}
    	}
    	return users_array[i].searchContact(contact_name);
    }
}
