/**
 * 
 */
package co.edu.unal.pos.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.DBConstants;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author andres.rojas
 *
 */
public class DBCommons {

	private final static Logger logger = Logger.getLogger(DBCommons.class);
	
	private static DBCommons INSTANCE = null;

	private static final ComboPooledDataSource cpds = new ComboPooledDataSource();

	static {
		buildConnectionPool();
	}
	
	public Connection getConnection() throws SQLException{
		return cpds.getConnection();
	}

	private static void buildConnectionPool() {
		try {
			cpds.setDriverClass(DBConstants.DRIVER_CLASS);
			cpds.setJdbcUrl(DBConstants.JDBC_URL);
			cpds.setUser(DBConstants.USER);
			cpds.setPassword(DBConstants.USER_PASSWORD);
			cpds.setMinPoolSize(5);
			cpds.setAcquireIncrement(5);
			cpds.setMaxPoolSize(20);
		} catch (Exception e) {
			logger.error("An error ocurred while building the connection pool to pos relational model",e);
		} 
	}

	public static DBCommons getInstance() {
		if (INSTANCE == null) {
			INSTANCE = new DBCommons();
		}
		return INSTANCE;
	}

	public void close(Connection connection) {
		if(connection!=null){
			try {
				connection.close();
			} catch (SQLException e) {
				logger.warn("error while closing the given connection");
			}
		}
	}

}