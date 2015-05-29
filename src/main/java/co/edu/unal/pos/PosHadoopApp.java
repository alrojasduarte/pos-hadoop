package co.edu.unal.pos;

import org.apache.log4j.Logger;

import co.edu.unal.pos.db.BillDetailDAO;


public class PosHadoopApp {

	private final static Logger logger = Logger.getLogger(PosHadoopApp.class);

	public static void main(String[] args) {
		BillDetailDAO billDetail  = new BillDetailDAO();
		int billDetailsCount = billDetail.countBillDetails();
		int batchSize = 5000;
		int batchEnd = billDetailsCount<batchSize?billDetailsCount:batchSize;
		for(int batchStart = 1;batchStart<=billDetailsCount;batchStart+=batchSize){
			batchEnd = batchStart+batchSize-1;
			if(batchEnd>billDetailsCount){
				batchEnd = billDetailsCount;
			}
			System.out.println(batchStart+"-"+(batchEnd));
			
		}
	}
}