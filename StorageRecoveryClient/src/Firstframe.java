import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
public class Firstframe extends JFrame implements ActionListener {
	private JLabel label1;
	private JPanel panel1;
	private JLabel label2;
	private JPanel panel2;
	private JPanel panel3;
	private JPanel panel4;
	private JPanel panel5;
	private JPanel panel6;
	private JTextField tf1;
	private JPasswordField pf1;
	private JLabel label3;
	private JLabel label4;
	private JButton button1;
	private JButton button2;
	private JButton button3;
	private JLabel label5;
	private JLabel label6;
	private JLabel label7;
	private JLabel label8;
	private JLabel label9;
	private JLabel label10;
	private JLabel label11;
	private JLabel label12;
	private JLabel label13;
	private JLabel label14;
	private JLabel label15;
	private JLabel label16;
	private JLabel label17;
	private JLabel label18;
	private JLabel label19;
	private JTextField tf2;
	private JPasswordField pf2;
	public Firstframe(){
		super("Phone Book App");
		setLayout(null);
		setSize(615,600);
		panel1=new JPanel();
		panel1.setBackground(Color.PINK);
		Color oraRed = new Color(156, 20, 20, 255);
		panel1.setBorder(BorderFactory.createLineBorder(oraRed, 10));
		label1=new JLabel("PhoneBook BackUp Application");
		label1.setFont(new Font("Brush Script MT", Font.BOLD, 40));
		label1.setForeground(Color.BLUE);
		panel1.add(label1);
		panel1.setBounds(0, 0, 600, 100);
		add(panel1);
		label2=new JLabel(new ImageIcon("image1.jpg"));
		label2.setBounds(400, 100, 200, 180);
		add(label2);
		label3=new JLabel("Username: ");
		label3.setFont(new Font("MV Boli",Font.BOLD, 20));
		label4=new JLabel("Password: ");
		label4.setFont(new Font("MV Boli",Font.BOLD, 20));
		tf1=new JTextField(20);
		pf1=new JPasswordField(20);
		panel2=new JPanel();
		//panel2.add(label3 ,FlowLayout.LEFT);
		//panel2.add(tf1);
		label3.setBounds(20,120,200,30);
		tf1.setBounds(135,125,200,25);
		add(label3);
		add(tf1);
		label4.setBounds(20,170,200,30);
		pf1.setBounds(135,175,200,25);
		add(label4);
		add(pf1);
		panel3=new JPanel();
		//panel3.add(label4 ,FlowLayout.LEFT);
		//panel3.add(pf1);
		panel2.setBounds(0, 115, 498, 30);
		panel3.setBounds(0, 160, 498 ,30);
		//add(panel2);
		//add(panel3);
		button1=new JButton(new ImageIcon("image17.png"));
		button2=new JButton(new ImageIcon("image18.png"));
		panel4=new JPanel();
		button1.addActionListener(this);
		button2.addActionListener(this);
		button1.setBounds(135,230,50,25);
		add(button1);
		button2.setBounds(230,230,50,25);
		add(button2);
		//panel4.add(button1);
		//panel4.add(button2);
		//panel4.setBounds(100, 160, 498, 42);
		//add(panel4);
		label5=new JLabel(new ImageIcon("image9.jpg"));
		label5.setBounds(0,312,480,80);
		add(label5);
		label6=new JLabel("Didn't you register yet?");
		label6.setBounds(0,302,600,40);
		label6.setFont(new Font("Times New Roman",Font.ITALIC,20));
		//add(label6);
		label7=new JLabel("Username: ");
		label7.setFont(new Font("Lucida Calligraphy",Font.CENTER_BASELINE, 20));
		label7.setBounds(20,380,200,80);
		add(label7);
		tf2=new JTextField(20);
		tf2.setBounds(20,440,150,25);
		add(tf2);
		label8=new JLabel("Password: ");
		label8.setFont(new Font("Lucida Calligraphy",Font.CENTER_BASELINE, 20));
		label8.setBounds(20,450,200,80);
		add(label8);
		pf2=new JPasswordField(20);
		pf2.setBounds(20,500,150,25);
		add(pf2);
		/*panel5=new JPanel();
		panel5.add(label7,FlowLayout.LEFT);
		panel5.add(tf2);
		panel6=new JPanel();
		panel6.add(label8,FlowLayout.LEFT);
		panel6.add(pf2);
		panel5.setBounds(250,350,258,65);
		panel6.setBounds(250,415,258,65);
		label9=new JLabel(new ImageIcon("image7.jpg"));
		label9.setBounds(0,342,225,225);*/
		//add(label9);
		//add(panel5);
		//add(panel6);
		label10=new JLabel(new ImageIcon("image21.png"));
		label10.setBounds(368,100,32,32);
		add(label10);
		label11=new JLabel(new ImageIcon("image21.png"));
		label11.setBounds(368,132,32,32);
		add(label11);
		label12=new JLabel(new ImageIcon("image21.png"));
		label12.setBounds(368,164,32,32);
		add(label12);
		label13=new JLabel(new ImageIcon("image21.png"));
		label13.setBounds(368,196,32,32);
		add(label13);
		label14=new JLabel(new ImageIcon("image21.png"));
		label14.setBounds(368,228,32,32);
		add(label14);
		label15=new JLabel(new ImageIcon("image21.png"));
		label15.setBounds(368,260,32,32);
		add(label15);
		button3=new JButton(new ImageIcon("image20.png"));
		button3.setBounds(200,440,307,84);
		button3.addActionListener(this);
		add(button3);
		setVisible(true);
	}
	public void actionPerformed(ActionEvent e){
		if(e.getSource()==button1){
	        String name=tf1.getText();
	    	char[] temp=pf1.getPassword();
	        String password_="";
	        for(int i=0;i<temp.length;++i){
	            password_=password_+Character.toString(temp[i]);
	        }
	        String user_password=Phonebook.findClient(name);
	        if(password_.equals(user_password)){
		    	tf1.setText("");
		        pf1.setText("");
		        new Secondframe(name);
	        }
	        else{
	        	if(user_password==null){
	        		JOptionPane.showMessageDialog(null, "No Such User!");
	        	}
	        	else{
	        		if(!password_.equals(user_password)){
	        			JOptionPane.showMessageDialog(null, "Wrong Username or Password! Try agian");
	        		}
	        	}
	        }
		}
		if(e.getSource()==button2){
	    	tf1.setText("");
	        pf1.setText("");
		}
		if(e.getSource()==button3){
	        String name=tf2.getText();
	        char[] temp=pf2.getPassword();
	        String password_="";
	        for(int i=0;i<temp.length;++i){
	            password_=password_+Character.toString(temp[i]);
	        }
	        String user_password=Phonebook.findClient(name);
	        if(user_password!=null){
	        	JOptionPane.showMessageDialog(null, "Username Exists!");
	        }
	        else{
	        	Phonebook.addClient(name, password_);
	        	Phonebook.addNewUser(name);
		        tf2.setText("");
		        pf2.setText("");    
	        }
		}
	}
}