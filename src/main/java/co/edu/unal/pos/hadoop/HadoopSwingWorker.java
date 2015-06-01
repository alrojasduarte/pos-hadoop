package co.edu.unal.pos.hadoop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.swing.SwingWorker;

import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.DBConnection;
import co.edu.unal.pos.common.constants.HadoopConstants;
import co.edu.unal.pos.common.constants.HadoopPropertiesKeys;
import co.edu.unal.pos.common.properties.PropertiesProvider;
import co.edu.unal.pos.db.DBCommons;
import co.edu.unal.pos.db.SalesFactsDAO;
import co.edu.unal.pos.gui.PosHadoopJFrame;

public class HadoopSwingWorker extends SwingWorker<Void, Void> {
	private final static Logger logger = Logger.getLogger(HadoopSwingWorker.class);
	
	private PosHadoopJFrame posHadoopJFrame;
	
	public HadoopSwingWorker(PosHadoopJFrame posHadoopJFrame) {
	 this.posHadoopJFrame = posHadoopJFrame;
	}
	@Override
	protected Void doInBackground() throws Exception {
		posHadoopJFrame.disableGUI();
		posHadoopJFrame.getEtlJProgressBar().setIndeterminate(true);
		posHadoopJFrame.getEtlJProgressBar().setStringPainted(true);
		posHadoopJFrame.getEtlJProgressBar().setString("Counting sales facts");
		posHadoopJFrame.getEtlJProgressBar().setValue(0);
		DBCommons dbCommons = new DBCommons();
		dbCommons.getConnection(DBConnection.POS_HADOOP);
		
		SalesFactsDAO salesFactsDAO = new SalesFactsDAO();
		int totalSalesFactsToProcess = salesFactsDAO.countSalesFacts();
		logger.info("the total sales facts to process is "+totalSalesFactsToProcess);
		posHadoopJFrame.getEtlJProgressBar().setString("The total sales facts to process is "+totalSalesFactsToProcess);
		int numberOfWorkers = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.NUBER_OF_WORKERS, HadoopConstants.NUMBER_OF_WORKERS);
		List<HadoopWorker> hadoopWorkers = new ArrayList<HadoopWorker>(numberOfWorkers);
		logger.info("The work will be do by "+numberOfWorkers+" workers");
		
		for (int i = 1; i <= numberOfWorkers; i++) {
			hadoopWorkers.add(new HadoopWorker());
		}

		ExecutorService executor = Executors.newFixedThreadPool(PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.NUBER_OF_WORKERS,HadoopConstants.NUMBER_OF_WORKERS));
		int from = 0;
		int batchSize = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.BATCH_SIZE,HadoopConstants.BATCH_SIZE);
		int to = batchSize;
		int totalSalesFactsProcessedCount = 0;
		posHadoopJFrame.getEtlJProgressBar().setIndeterminate(false);
		Double progressPercentaje = 0D;
		while(to<totalSalesFactsToProcess){
			logger.info("from="+from+",to="+to);
			try {
				int i = 0;
				for(HadoopWorker hadoopWorker:hadoopWorkers){
					logger.info("from="+from+",to="+to);
					hadoopWorker.setFrom(from);
					hadoopWorker.setTo(to);					
				    from += batchSize;
				    to+=batchSize;
				    i++;
				    if(to>totalSalesFactsToProcess){
				    	hadoopWorker.setTo(totalSalesFactsToProcess);
				    	int j = hadoopWorkers.size()-1;
				    	while(j>=i){
				    		hadoopWorkers.remove(j);
				    		j--;
				    	}
				    	break;
				    }
				}
				List<Future<HadoopWorkerResult>> results = executor.invokeAll(hadoopWorkers);
				Iterator<Future<HadoopWorkerResult>> resultsIterator = results.iterator();
				while(resultsIterator.hasNext()){
					Future<HadoopWorkerResult> result = resultsIterator.next();
					totalSalesFactsProcessedCount+=result.get().getSalesFactsProcessedCount();
				}
				logger.info("the total sales facts processed count is "+totalSalesFactsProcessedCount);
				progressPercentaje = ((double)totalSalesFactsProcessedCount)/((double)totalSalesFactsToProcess);
				progressPercentaje *= 100;
				logger.info("progress percentaje="+(progressPercentaje.intValue()*100));
				posHadoopJFrame.getEtlJProgressBar().setValue(progressPercentaje.intValue());
				posHadoopJFrame.getEtlJProgressBar().setString(progressPercentaje.intValue()+" % sales facts processed ");
			} catch (Exception e) {
				logger.error("error while invoking hadoop workers", e);
			}	
		}
		posHadoopJFrame.getEtlJProgressBar().setValue(100);
		posHadoopJFrame.getEtlJProgressBar().setString("100 % sales facts processed ");
		logger.info("finished etl stage with "+totalSalesFactsProcessedCount+" sales facts processed ");
		posHadoopJFrame.enableGUI();

		return null;
	}

}
