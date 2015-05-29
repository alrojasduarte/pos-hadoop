package co.edu.unal.pos;

import java.sql.Connection;

import javax.swing.JFrame;

import org.apache.log4j.Logger;

import co.edu.unal.pos.db.DBCommons;

public class PosHadoopApp {

	private final static Logger logger = Logger.getLogger(PosHadoopApp.class);

	public static void main(String[] args) {
		JFrame jFrame = new JFrame();
		jFrame.show();
		jFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		
		logger.info("Testing connection");
		Connection connection = null;
		try{
			connection = DBCommons.getInstance().getConnection();
			logger.info("Testing connection success");
		}catch(Exception e){
			logger.error("The connection tested failed",e);
		}finally{
			DBCommons.getInstance().close(connection);
		}
		
	}
}