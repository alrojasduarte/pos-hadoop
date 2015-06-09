/**
 * 
 */
package co.edu.unal.pos.model;

/**
 * @author andres.rojas
 *
 */
public class SaleFact {

	private Long id;
	private ProductDimension productDimension;
	private Integer productQuantity;
	private Integer price;
	private TimeDimension timeDimension;
	
	public SaleFact(ProductDimension productDimension,Integer productQuantity, TimeDimension timeDimension){
		this.productDimension = productDimension;
		this.productQuantity = productQuantity;
		this.timeDimension = timeDimension;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public ProductDimension getProductDimension() {
		return productDimension;
	}

	public void setProductDimension(ProductDimension productDimension) {
		this.productDimension = productDimension;
	}

	public Integer getProductQuantity() {
		return productQuantity;
	}

	public void setProductQuantity(Integer productQuantity) {
		this.productQuantity = productQuantity;
	}

	public TimeDimension getTimeDimension() {
		return timeDimension;
	}

	public void setTimeDimension(TimeDimension timeDimension) {
		this.timeDimension = timeDimension;
	}

	public Integer getPrice() {
		return price;
	}

	public void setPrice(Integer price) {
		this.price = price;
	}

	
		
	
}