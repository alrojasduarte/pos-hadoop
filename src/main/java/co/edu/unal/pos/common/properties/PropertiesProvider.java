package co.edu.unal.pos.common.properties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.DBConnection;
import co.edu.unal.pos.common.constants.SqlQueries;
import co.edu.unal.pos.db.DBCommons;

public class PropertiesProvider {

	private final static Logger logger = Logger.getLogger(PropertiesProvider.class);
	
	private static PropertiesProvider INSTANCE;
	
	private DBCommons dbCommons = new DBCommons();
	
	private Map<String,String> properties = new TreeMap<String, String>();
	
	private PropertiesProvider(){
		getProperties();
	}
	
	public Map<String,String> getProperties() {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try{
			connection = dbCommons.getConnection(DBConnection.POS_HADOOP);
			preparedStatement = connection.prepareStatement(SqlQueries.SELECT_PROPERTIES);
			resultSet = preparedStatement.executeQuery();
			while(resultSet.next()){
				properties.put(resultSet.getString(1), resultSet.getString(2));
			}
		}catch(Exception e){		
			logger.info("an error ocurred while trying to get the properties ",e);
		}finally{
			dbCommons.close(connection);
			dbCommons.close(preparedStatement);
			dbCommons.close(resultSet);
		}
		return properties;
	}

	public void saveProperty(String key,String value){
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try{
			connection = dbCommons.getConnection(DBConnection.POS_HADOOP);
			connection.setAutoCommit(false);
			deleteProperty(key,connection);
			preparedStatement = connection.prepareStatement(SqlQueries.SAVE_PROPERTY);
			preparedStatement.setString(1, key);
			preparedStatement.setString(2, value);
			preparedStatement.execute();
			connection.commit();
		}catch(Exception e){
			logger.info("an error ocurred while trying to save the property "+key,e);
		}finally{
			dbCommons.close(connection);
			dbCommons.close(preparedStatement);
		}
	}
	
	private void deleteProperty(String key,Connection connection) {
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = connection.prepareStatement(SqlQueries.DELETE_PROPERTY);
			preparedStatement.setString(1, key);
			preparedStatement.execute();
		}catch(Exception e){
			logger.info("an error ocurred while trying to delete the property "+key,e);
		}finally{
			dbCommons.close(preparedStatement);
		}
	}

	public String getProperty(String key,String defaultValue){
		String value = properties.get(key);
		if(value == null){
			saveProperty(key, defaultValue);
			properties.put(key,value);
			value = defaultValue;

		}
		return value;
	}

	
	public static PropertiesProvider getInstance(){
		if(INSTANCE==null){
			INSTANCE = new PropertiesProvider();
		}
		return INSTANCE;
	}

	public int getPropertyAsInt(String key, int defaultValue) {
		return Integer.parseInt(getProperty(key, String.valueOf(defaultValue)));
	}

	public void saveProperties(Map<String, String> properties) {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		try{
			connection = dbCommons.getConnection(DBConnection.POS_HADOOP);
			connection.setAutoCommit(false);
			preparedStatement = connection.prepareStatement(SqlQueries.SAVE_PROPERTY);
			for(Map.Entry<String,String> property:properties.entrySet()){
				deleteProperty(property.getKey(),connection);
				preparedStatement.setString(1, property.getKey());
				preparedStatement.setString(2, property.getValue());
				preparedStatement.addBatch();				
			}
			preparedStatement.executeBatch();
			connection.commit();
			this.properties.clear();
			this.properties.putAll(properties);
		}catch(Exception e){
			logger.info("an error ocurred while trying to save the properties",e);
		}finally{
			dbCommons.close(connection);
			dbCommons.close(preparedStatement);
		}
	}
	
}
