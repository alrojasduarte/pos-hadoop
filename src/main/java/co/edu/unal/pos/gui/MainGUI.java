/**
 * 
 */
package co.edu.unal.pos.gui;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import co.edu.unal.pos.model.FactSales;

/**
 * @author andres.rojas
 *
 */
public class MainGUI {

	private void doGui() {
		System.out.println(System.getProperty("java.awt.headless"));
    	System.setProperty("java.awt.headless", "false");
    	System.out.println(System.getProperty("java.awt.headless"));
		   System.out.println( "Hello World! From RUN!!!" );
		   JFrame frame = new JFrame("POS Hadoop DataStore");

			JPanel panel = new JPanel();
			panel.setLayout(new FlowLayout());

			JLabel label = new JLabel("Test Write");

			JButton button = new JButton();
			button.setText("Test Write");
			button.addActionListener(new ActionListener() {
				
				@Override
				public void actionPerformed(ActionEvent e) {
					System.out.println("Writing to Hadoop");
					try {
						writer.write(new FactSales("1234#","1234"));
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					System.out.println("Done Writing to Hadoop");
				}
			});

			panel.add(label);
			panel.add(button);

			frame.add(panel);
			frame.setSize(300, 300);
			frame.setLocationRelativeTo(null);
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
			frame.setVisible(true);
	}
	
}
