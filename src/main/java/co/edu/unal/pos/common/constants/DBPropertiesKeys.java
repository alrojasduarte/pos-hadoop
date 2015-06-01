/**
 * 
 */
package co.edu.unal.pos.common.constants;

/**
 * @author Andres
 *
 */
public interface DBPropertiesKeys {
	String NAME_SPACE="db.";
	
	String POS_RM_USER=NAME_SPACE+PropertiesKeys.NAME_SPACE+"posRmUser";
	String POS_RM_USER_PASSWORD = NAME_SPACE+PropertiesKeys.NAME_SPACE+"posRmPassword";	
	String POS_RM_JDBC_URL=NAME_SPACE+PropertiesKeys.NAME_SPACE+"posRmJdbcUrl";
	
	String POS_HADOOP_USER=NAME_SPACE+PropertiesKeys.NAME_SPACE+"posHadoopUser";
	String POS_HADOOP_USER_PASSWORD = NAME_SPACE+PropertiesKeys.NAME_SPACE+"posHadoopPassword";	
	String POS_HADOOP_JDBC_URL=NAME_SPACE+PropertiesKeys.NAME_SPACE+"posHadoopJdbcUrl";

	
}
