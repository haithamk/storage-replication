import java.awt.*;
import java.awt.event.*;

import javax.swing.*;

public class Fourthframe extends JFrame implements ActionListener{
	private JLabel label1;
	private JLabel label2;
	private JLabel label3;
	private JLabel label4;
	private JTextField tf1;
	private JTextField tf2;
	private JButton button1;
	private JButton button2;
	private String user_name;
	public Fourthframe(String user_name){
		this.user_name=user_name;
		setLayout(null);
		setSize(400,400);
		label4=new JLabel("Modify Contact");
		label4.setFont(new Font("Tempus Sans ITC",Font.BOLD,20));
		label4.setBounds(140, 5, 150, 25);
		add(label4);
		label1=new JLabel(new ImageIcon("image12.jpg"));
		label1.setBounds(140,30,102,102);
		add(label1);
		label2=new JLabel("Contact name: ");
		label2.setFont(new Font("Tempus Sans ITC",Font.CENTER_BASELINE,14));
		label2.setBounds(20,190,120,30);
		add(label2);
		tf1=new JTextField(20);
		tf1.setBounds(145,195,200,25);
		add(tf1);
		label3=new JLabel("Modified number: ");
		label3.setFont(new Font("Tempus Sans ITC",Font.CENTER_BASELINE,14));
		label3.setBounds(20,240,150,30);
		add(label3);
		tf2=new JTextField(20);
		tf2.setBounds(145,245,200,25);
		add(tf2);
		button1=new JButton("Modify");
		button1.setFont(new Font("Brush Script MT",Font.BOLD,20));
		button1.setBounds(240,280,100,30);
		button1.addActionListener(this);
		add(button1);
		button2=new JButton(new ImageIcon("image16.png"));
		button2.setBounds(0,0,50,50);
		button2.addActionListener(this);
		add(button2);
		setVisible(true);
	}
	public void actionPerformed(ActionEvent e){
		if(e.getSource()==button1){
			String contact_name=tf1.getText();
			Integer contact_number=Integer.parseInt(tf2.getText());
			tf1.setText("");
			tf2.setText("");
			Phonebook.addNewContact(user_name, contact_name, contact_number);
			JOptionPane.showMessageDialog(null, "Contact modified");
		}
		if(e.getSource()==button2){
			this.dispose();
		}
	}
}
