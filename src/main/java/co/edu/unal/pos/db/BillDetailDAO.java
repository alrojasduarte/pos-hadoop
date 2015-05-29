/**
 * 
 */
package co.edu.unal.pos.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.SqlQueries;


/**
 * @author andres.rojas
 *
 */
public class BillDetailDAO {
	
	private final static Logger logger = Logger.getLogger(BillDetailDAO.class);
	
	public int countBillDetails(){
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		int billDetailsCount = 0;
		try{
			logger.info("getting bill details count");
			connection = DBCommons.getInstance().getConnection();
			preparedStatement = connection.prepareStatement(SqlQueries.COUNT_BILL_DETAILS);
			resultSet = preparedStatement.executeQuery();
			resultSet.next();
			billDetailsCount =  resultSet.getInt(1);
			logger.info("the bill details count is "+billDetailsCount);
			return billDetailsCount;
		}catch(Exception e){
			logger.error("An error ocurred while trying to get the bill details count",e);
		}finally{
			DBCommons.getInstance().close(resultSet);
			DBCommons.getInstance().close(preparedStatement);
			DBCommons.getInstance().close(connection);
		}
		return billDetailsCount;
	}
	
}