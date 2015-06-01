package co.edu.unal.pos.hadoop;

import co.edu.unal.pos.common.constants.HadoopWorkResult;

public class HadoopWorkerResult {

	private HadoopWorkResult hadoopWorkResult;
	private int salesFactsProcessedCount;

	public HadoopWorkerResult(HadoopWorkResult hadoopWorkResult, int salesFactsProcessedCount) {
		this.hadoopWorkResult = hadoopWorkResult;
		this.salesFactsProcessedCount = salesFactsProcessedCount;
	}

	public HadoopWorkerResult() {
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
