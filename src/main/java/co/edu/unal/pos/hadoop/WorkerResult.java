package co.edu.unal.pos.hadoop;

import co.edu.unal.pos.common.constants.HadoopWorkResult;

public class WorkerResult {

	private HadoopWorkResult hadoopWorkResult;
	private int salesFactsProcessedCount;

	public WorkerResult(HadoopWorkResult hadoopWorkResult, int salesFactsProcessedCount) {
		this.hadoopWorkResult = hadoopWorkResult;
		this.salesFactsProcessedCount = salesFactsProcessedCount;
	}

	public WorkerResult() {
		// TODO Auto-generated constructor stub
	}

	public HadoopWorkResult getHadoopWorkResult() {
		return hadoopWorkResult;
	}

	public void setHadoopWorkResult(HadoopWorkResult hadoopWorkResult) {
		this.hadoopWorkResult = hadoopWorkResult;
	}

	public int getSalesFactsProcessedCount() {
		return salesFactsProcessedCount;
	}

	public void setSalesFactsProcessedCount(int salesFactsProcessedCount) {
		this.salesFactsProcessedCount = salesFactsProcessedCount;
	}
	
	
}
