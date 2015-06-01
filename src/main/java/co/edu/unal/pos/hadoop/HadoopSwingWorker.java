package co.edu.unal.pos.hadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
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
//		int totalSalesFactsToProcess = 50000;
		logger.info("the total sales facts to process is "+totalSalesFactsToProcess);
		posHadoopJFrame.getEtlJProgressBar().setString("The total sales facts to process is "+totalSalesFactsToProcess);
		int numberOfWorkers = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.NUBER_OF_WORKERS, HadoopConstants.NUMBER_OF_WORKERS);
		List<HadoopWorker> hadoopWorkers = new ArrayList<HadoopWorker>(numberOfWorkers);
		logger.info("The work will be do by "+numberOfWorkers+" workers");		
		ExecutorService executor = Executors.newFixedThreadPool(PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.NUBER_OF_WORKERS,HadoopConstants.NUMBER_OF_WORKERS));
		CompletionService<HadoopWorkerResult> completionService = new ExecutorCompletionService<HadoopWorkerResult>(executor);
		int from = 0;
		int batchSize = PropertiesProvider.getInstance().getPropertyAsInt(HadoopPropertiesKeys.BATCH_SIZE,HadoopConstants.BATCH_SIZE);
		int to = batchSize;
		int totalSalesFactsProcessedCount = 0;
		posHadoopJFrame.getEtlJProgressBar().setIndeterminate(false);
		Double progressPercentaje = 0D;
		int numberOfRequestedThreads = 0;
		HadoopClient.getInstance().openPath();
		while(to<=totalSalesFactsToProcess){
			logger.info("from="+from+",to="+to);			
			completionService.submit(new HadoopWorker(from, to));			
			from+=batchSize;
			to+=batchSize;
			numberOfRequestedThreads++;
			if(to>totalSalesFactsToProcess){
				to = totalSalesFactsToProcess;
				completionService.submit(new HadoopWorker(from, to));	
				break;
			}			
		}
		logger.info("done making workers "+numberOfRequestedThreads);
		while(numberOfRequestedThreads-->0){
			logger.info("numberOfRequestedThreads="+numberOfRequestedThreads);
			Future<HadoopWorkerResult> future = completionService.take();
			logger.info("taked!!");
			HadoopWorkerResult hadoopWorkerResult = future.get();
			totalSalesFactsProcessedCount+=hadoopWorkerResult.getSalesFactsProcessedCount();
			progressPercentaje = ((double)totalSalesFactsProcessedCount)/((double)totalSalesFactsToProcess);
			progressPercentaje *= 100;
			logger.info("progressPercentaje="+progressPercentaje);
			posHadoopJFrame.getEtlJProgressBar().setValue(progressPercentaje.intValue());
			posHadoopJFrame.getEtlJProgressBar().setString(progressPercentaje.intValue()+" % sales facts processed ");
		}
		HadoopClient.getInstance().closePath();
		posHadoopJFrame.getEtlJProgressBar().setValue(100);
		posHadoopJFrame.getEtlJProgressBar().setString("100 % sales facts processed ");
		logger.info("finished etl stage with "+totalSalesFactsProcessedCount+" sales facts processed ");
		posHadoopJFrame.enableGUI();

		return null;
	}

}
