/**
 * 
 */
package co.edu.unal.pos.db;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.DBConnection;
import co.edu.unal.pos.common.constants.DBConstants;
import co.edu.unal.pos.common.constants.DBPropertiesKeys;
import co.edu.unal.pos.common.properties.PropertiesProvider;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author andres.rojas
 *
 */
public class DBCommons {

	private final static Logger logger = Logger.getLogger(DBCommons.class);
	
	private static DBCommons INSTANCE = null;

	private static Map<DBConnection,ComboPooledDataSource> connectionPoolsByConnection = null;

	private static Map<DBConnection,String[]> connectionDataByConnection = null;
	
	public DBCommons(){
		buildConnectionPools();
	}
	
	public Connection getConnection(DBConnection dbConnection) throws SQLException, PropertyVetoException{
		try{
			logger.info("getting connection to "+dbConnection);
			Connection connection = connectionPoolsByConnection.get(dbConnection).getConnection();
			logger.info("successfull connection to "+dbConnection);
			return connection;
		}catch(SQLException e){
			logger.info("error while trying to connecto to pos relational model");
			throw e;
		}
		
	}

	
	private void buildConnectionPools() {
		try{
			if(connectionDataByConnection==null){
				logger.info("building connection pools");
				connectionDataByConnection = new HashMap<DBConnection, String[]>();
				connectionDataByConnection.put(DBConnection.POS_RM, new String[]{DBConstants.POS_RM_JDBC_URL,DBConstants.POS_RM_USER, DBConstants.POS_RM_USER_PASSWORD});
				connectionDataByConnection.put(DBConnection.POS_HADOOP, new String[]{DBConstants.POS_HADOOP_JDBC_URL,DBConstants.POS_HADOOP_USER, DBConstants.POS_HADOOP_USER_PASSWORD});
				connectionPoolsByConnection = new HashMap<DBConnection, ComboPooledDataSource>();
				for(Map.Entry<DBConnection, String[]> connectionDataEntry:connectionDataByConnection.entrySet()){
					ComboPooledDataSource  comboPooledDataSource = new ComboPooledDataSource();
					comboPooledDataSource.setDriverClass(DBConstants.DRIVER_CLASS);
					comboPooledDataSource.setJdbcUrl(connectionDataEntry.getValue()[0]);
					comboPooledDataSource.setUser(connectionDataEntry.getValue()[1]);
					comboPooledDataSource.setPassword(connectionDataEntry.getValue()[2]);
					comboPooledDataSource.setMinPoolSize(5);
					comboPooledDataSource.setAcquireIncrement(5);
					comboPooledDataSource.setMaxPoolSize(20);
					connectionPoolsByConnection.put(connectionDataEntry.getKey(), comboPooledDataSource);
					logger.info("build connection pool for "+connectionDataEntry.getKey());
				}				
			}
		}catch(Exception e){
			logger.error("erro while trying to build database connection pools",e);
		}
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

	public void close(PreparedStatement preparedStatement) {
		if(preparedStatement!=null){
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				logger.warn("error while closing the given prepared statement");
			}
		}
		
	}
	
	public void close(ResultSet resultSet) {
		if(resultSet!=null){
			try {
				resultSet.close();
			} catch (SQLException e) {
				logger.warn("error while closing the given result set");
			}
		}
		
	}

}