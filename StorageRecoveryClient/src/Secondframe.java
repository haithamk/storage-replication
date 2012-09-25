import java.awt.*;
import java.awt.event.*;

import javax.swing.*;
public class Secondframe extends JFrame implements ActionListener{
	private JLabel label1;
	private JLabel label2;
	private JPanel panel1;
	private JPanel panel2;
	private JButton button1;
	private JButton button2;
	private JButton button3;
	private JButton button4;
	private String user_name;
	public Secondframe(String user_name){
		super("Phone Book App.");
		this.user_name=user_name;
		setLayout(null);
		setSize(500,400);
		label1=new JLabel(new ImageIcon("image15.png"));
		label1.setBounds(0,0,102,102);
		label2=new JLabel("Phone Book Application");
		label2.setBounds(110,40,370,40);
		Color color1=new Color(196,115,196);
		label2.setForeground(color1);
		label2.setFont(new Font("Lucida Calligraphy",Font.BOLD,25));
		add(label1);
		add(label2);
		button1=new JButton("Add Contact",new ImageIcon("image13.png"));
		button1.setFont(new Font("Lucida Calligraphy",Font.BOLD,13));
		button1.setBounds(2,140,145,150);
		button1.setHorizontalTextPosition(SwingConstants.CENTER);
		button1.setVerticalTextPosition(SwingConstants.BOTTOM);
		button1.addActionListener(this);
		add(button1);
		button2=new JButton("Modify Contact",new ImageIcon("image12.jpg"));
		button2.setFont(new Font("Lucida Calligraphy",Font.BOLD,13));
		button2.setBounds(152,140,162,150);
		button2.setHorizontalTextPosition(SwingConstants.CENTER);
		button2.setVerticalTextPosition(SwingConstants.BOTTOM);
		button2.addActionListener(this);
		add(button2);
		button3=new JButton("Search Contact",new ImageIcon("image14.png"));
		button3.setFont(new Font("Lucida Calligraphy",Font.BOLD,13));
		button3.setBounds(320,140,160,150);
		button3.setHorizontalTextPosition(SwingConstants.CENTER);
		button3.setVerticalTextPosition(SwingConstants.BOTTOM);
		button3.addActionListener(this);
		add(button3);
		button4=new JButton(new ImageIcon("image16.png"));
		button4.setBounds(445,0,40,40);
		button4.addActionListener(this);
		add(button4);
		setVisible(true);
	}
	public void actionPerformed(ActionEvent e){
		if(e.getSource()==button1){
			new Thirdframe(user_name);
		}
		if(e.getSource()==button2){
			new Fourthframe(user_name);
		}
		if(e.getSource()==button3){
			new Fifthframe(user_name);
		}
		if(e.getSource()==button4){
			this.dispose();
		}
	}
}
