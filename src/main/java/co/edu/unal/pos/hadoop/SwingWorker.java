package co.edu.unal.pos.hadoop;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import co.edu.unal.pos.common.constants.DBConnection;
import co.edu.unal.pos.common.constants.HadoopConstants;
import co.edu.unal.pos.common.constants.HadoopPropertiesKeys;
import co.edu.unal.pos.common.constants.PropertiesKeys;
import co.edu.unal.pos.common.properties.PropertiesProvider;
import co.edu.unal.pos.db.DBCommons;
import co.edu.unal.pos.db.SalesFactsDAO;
import co.edu.unal.pos.gui.PosHadoopJFrame;

public class SwingWorker extends javax.swing.SwingWorker<Void, Void> {
	private final static Logger logger = Logger.getLogger(SwingWorker.class);
	
	private PosHadoopJFrame posHadoopJFrame;
	
	public SwingWorker(PosHadoopJFrame posHadoopJFrame) {
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
		boolean testMode = PropertiesProvider.getInstance().getPropertyAsBoolean(PropertiesKeys.TEST_MODE, false);
		int totalSalesFactsToProcess = 5000;
		if(!testMode){
			totalSalesFactsToProcess = salesFactsDAO.countSalesFacts();			
		}
		
		
		logger.info("the total sales facts to process is "+totalSalesFactsToProcess);
		posHadoopJFrame.getEtlJProgressBar().setString("The total sales facts to process is "+totalSalesFactsToProcess);
		int numberOfWorkers = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.NUBER_OF_WORKERS, HadoopConstants.NUMBER_OF_WORKERS);
		logger.info("The work will be do by "+numberOfWorkers+" workers");		
		ExecutorService executor = Executors.newFixedThreadPool(PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.NUBER_OF_WORKERS,HadoopConstants.NUMBER_OF_WORKERS));
		CompletionService<WorkerResult> completionService = new ExecutorCompletionService<WorkerResult>(executor);
		int from = 0;
		int batchSize = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.BATCH_SIZE,HadoopConstants.BATCH_SIZE);
		int to = batchSize;
		int totalSalesFactsProcessedCount = 0;
		posHadoopJFrame.getEtlJProgressBar().setIndeterminate(false);
		Double progressPercentaje = 0D;
		int numberOfRequestedThreads = 0;
		WriterClient.getInstance().openPath();
		while(to<=totalSalesFactsToProcess){
//			logger.info("from="+from+",to="+to);			
			completionService.submit(new Worker(from, to));			
			from+=batchSize;
			to+=batchSize;
			numberOfRequestedThreads++;
			if(to>totalSalesFactsToProcess){
				to = totalSalesFactsToProcess;
				completionService.submit(new Worker(from, to));	
				break;
			}			
		}
		logger.info("done making workers "+numberOfRequestedThreads);
		while(numberOfRequestedThreads>=0){
			logger.info("numberOfRequestedThreads="+numberOfRequestedThreads);
			Future<WorkerResult> future = completionService.take();
			logger.info("taked!!");
			WorkerResult hadoopWorkerResult = future.get();
			totalSalesFactsProcessedCount+=hadoopWorkerResult.getSalesFactsProcessedCount();
			progressPercentaje = ((double)totalSalesFactsProcessedCount)/((double)totalSalesFactsToProcess);
			progressPercentaje *= 100;
			logger.info("progressPercentaje="+progressPercentaje);
			posHadoopJFrame.getEtlJProgressBar().setValue(progressPercentaje.intValue());
			posHadoopJFrame.getEtlJProgressBar().setString(progressPercentaje.intValue()+" % sales facts processed ");
			numberOfRequestedThreads--;			
		}
		JobRunner.getInstance().setInputPath(WriterClient.getInstance().getCurrentPath());
		WriterClient.getInstance().closePath();
		posHadoopJFrame.getEtlJProgressBar().setValue(100);
		posHadoopJFrame.getEtlJProgressBar().setString("100 % sales facts processed ");
		logger.info("finished etl stage with "+totalSalesFactsProcessedCount+" sales facts processed ");
		posHadoopJFrame.enableGUI();

		return null;
	}

}
