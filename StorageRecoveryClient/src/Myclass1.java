
public class Myclass1 {
	String user_name;
	Integer index;
	Myclass2 contacts[];
	public Myclass1(String name){
		user_name=name;
		index=0;
		contacts=new Myclass2[20];
	}
	void addContact(String name ,Integer number){
		contacts[index]=new Myclass2(name ,number);
		++index;
	}
	void modifyContact(String name ,Integer number){
		for(int i=0;i<index;++i){
			if(name.equals(contacts[i].contact_name)){
				contacts[i].contact_number=number;
				break;
			}
		}
	}
	Integer searchContact(String name){
		Integer number = 0;
		for(int i=0;i<index;++i){
			if(name.equals(contacts[i].contact_name)){
				number = contacts[i].contact_number;
			}
		}
		return number;
	}
}
