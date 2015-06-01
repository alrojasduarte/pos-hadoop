package co.edu.unal.pos.common.constants;

public interface SqlQueries {

	String COUNT_BILL_DETAILS = 
			" SELECT COUNT(1) "+
			" FROM detallefacturas AS df "+
			" JOIN facturas AS f ON (f.nofactura = df.nofactura AND f.caja = df.caja) "+
			" JOIN articulos AS a ON a.codigoarticulo = df.codigoarticulo ";

	String SELECT_SALES_FACTS =
			" SELECT df.codigoarticulo,a.descripcion,df.cantidad,YEAR(f.fechafactura),MONTH(f.fechafactura),DAYOFMONTH(f.fechafactura) "+
			" FROM detallefacturas AS df "+
			" JOIN facturas AS f ON (f.nofactura = df.nofactura AND f.caja = df.caja) "+
			" JOIN articulos AS a ON a.codigoarticulo = df.codigoarticulo "+
			" LIMIT ? , ? ";
	
	String DELETE_PROPERTY=" DELETE FROM property WHERE key_ = ? ";

	String SAVE_PROPERTY = " INSERT INTO property (key_,value_) VALUES (?,?) ";

	String SELECT_PROPERTIES = " SELECT key_,value_ FROM property ";
	
}
