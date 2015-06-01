/**
 * 
 */
package co.edu.unal.pos.model;

/**
 * @author andres.rojas
 *
 */
public class SaleFact {

	private long id;
	private ProductDimension productDimension;
	private int productQuantity;
	private BillDimension billDimension;
	
	public SaleFact(ProductDimension productDimension,int productQuantity, BillDimension billDimension){
		this.productDimension = productDimension;
		this.productQuantity = productQuantity;
		this.billDimension = billDimension;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public ProductDimension getProductDimension() {
		return productDimension;
	}

	public void setProductDimension(ProductDimension productDimension) {
		this.productDimension = productDimension;
	}

	public int getProductQuantity() {
		return productQuantity;
	}

	public void setProductQuantity(int productQuantity) {
		this.productQuantity = productQuantity;
	}

	public BillDimension getBillDimension() {
		return billDimension;
	}

	public void setBillDimension(BillDimension billDimension) {
		this.billDimension = billDimension;
	}
	
	
}