/**
 * 
 */
package co.edu.unal.pos.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.DBConnection;
import co.edu.unal.pos.common.constants.SqlQueries;
import co.edu.unal.pos.model.TimeDimension;
import co.edu.unal.pos.model.ProductDimension;
import co.edu.unal.pos.model.SaleFact;


/**
 * @author andres.rojas
 *
 */
public class SalesFactsDAO {
	
	private final static Logger logger = Logger.getLogger(SalesFactsDAO.class);
	
	private DBCommons dbCommons = new DBCommons();
	
	public int countSalesFacts(){
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		int billDetailsCount = 0;
		try{
			logger.info("getting bill details count");
			connection = dbCommons.getConnection(DBConnection.POS_RM);
			preparedStatement = connection.prepareStatement(SqlQueries.COUNT_BILL_DETAILS);
			resultSet = preparedStatement.executeQuery();
			resultSet.next();
			billDetailsCount =  resultSet.getInt(1);
			logger.info("the bill details count is "+billDetailsCount);
			return billDetailsCount;
		}catch(Exception e){
			logger.error("An error ocurred while trying to get the bill details count",e);
		}finally{
			dbCommons.close(resultSet);
			dbCommons.close(preparedStatement);
			dbCommons.close(connection);
		}
		return billDetailsCount;
	}
	
	public List<SaleFact> getSalesFacts(int from,int to){
		List<SaleFact> salesFacts = new ArrayList<SaleFact>();
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try{
			logger.info("getting sales facts from "+from+" to "+to);
			connection = dbCommons.getConnection(DBConnection.POS_RM);
			preparedStatement = connection.prepareStatement(SqlQueries.SELECT_SALES_FACTS);
			preparedStatement.setInt(1, from);
			preparedStatement.setInt(2, to-from);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				ProductDimension productDimension = new ProductDimension(resultSet.getString(1), resultSet.getString(2));
				TimeDimension  billDimension = new TimeDimension(resultSet.getInt(5), resultSet.getInt(6), resultSet.getInt(7));
				SaleFact salesFact = new SaleFact(productDimension,resultSet.getInt(3),billDimension);
				salesFact.setPrice(resultSet.getInt(4));
				salesFacts.add(salesFact);
				
			}
			logger.info("the sales facts count from "+from+" to "+to+" is "+salesFacts.size());
		}catch(Exception e){
			logger.error("an error ocurred while trying to get the sales facts", e);
		}finally{
			dbCommons.close(resultSet);
			dbCommons.close(preparedStatement);
			dbCommons.close(connection);
		}
		return salesFacts;
	}
	
}