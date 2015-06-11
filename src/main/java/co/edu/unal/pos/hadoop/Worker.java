package co.edu.unal.pos.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import co.edu.unal.pos.common.constants.HadoopWorkResult;
import co.edu.unal.pos.db.SalesFactsDAO;
import co.edu.unal.pos.model.SaleFact;
//http://noushinb.blogspot.com/2013/04/reading-writing-hadoop-sequence-files.html
public class Worker implements Callable<WorkerResult> {
	
	private final static Logger logger = Logger.getLogger(Worker.class);


	private int from;
	private int to;


	private SalesFactsDAO salesFactsDAO = new SalesFactsDAO();
	private WorkerResult hadoopWorkerResult = new WorkerResult();
	private Gson gson = new Gson();

	public Worker(int from, int to){
		this.from = from;
		this.to = to;
	
	}
	
	public Worker() {
		// TODO Auto-generated constructor stub
	}

	public WorkerResult call() throws Exception {
		logger.info("procesing sales facts from "+from+" to "+to);
		try{

			List<SaleFact> salesFacts = salesFactsDAO.getSalesFacts(from, to);
			for(SaleFact saleFact:salesFacts){
				writeSaleFact(saleFact);
			}
			hadoopWorkerResult.setHadoopWorkResult(HadoopWorkResult.OK);
			hadoopWorkerResult.setSalesFactsProcessedCount(salesFacts.size());
			logger.info("finished procesing sales facts from "+from+" to "+to);
		}catch(Exception e){
			logger.error("an error ocurred while trying to processes sales facts from "+from+" to "+to,e);
			hadoopWorkerResult.setHadoopWorkResult(HadoopWorkResult.ERROR);
			return hadoopWorkerResult;
		}		
		logger.info("procesed sales facts from "+from+" to "+to);
		return hadoopWorkerResult;
	}

	private void writeSaleFact(SaleFact saleFact) throws IOException {		
		WriterClient.getInstance().write(gson.toJson(saleFact));
//		logger.info(gson.toJson(saleFact));
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getTo() {
		return to;
	}

	public void setTo(int to) {
		this.to = to;
	}
	
	
}