/**
 * 
 */
package co.edu.unal.pos.model;

/**
 * @author andres.rojas
 *
 */
public class FactSales {

	private String billNumber="";
	
	private String id="";
	
	public  FactSales(){
		
	}
	
	public FactSales(String billNumber,String id){
		this.billNumber=billNumber;
		this.id = id;
	}
	
}
