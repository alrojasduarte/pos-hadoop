package co.edu.unal.pos.common.constants;

public interface SqlQueries {

	String COUNT_BILL_DETAILS = 
			" SELECT COUNT(1) "+
			" FROM detallefacturas AS df "+
			" JOIN facturas AS f ON (f.nofactura = df.nofactura AND f.caja = df.caja) "+
			" JOIN articulos AS a ON a.codigoarticulo = df.codigoarticulo ";

	String SELECT_BILL_DETAILS =
			" SELECT df.codigoarticulo,a.descripcion,df.cantidad,YEAR(f.fechafactura),MONTH(f.fechafactura),DAYOFMONTH(f.fechafactura) "+
			" FROM detallefacturas AS df "+
			" JOIN facturas AS f ON (f.nofactura = df.nofactura AND f.caja = df.caja) "+
			" JOIN articulos AS a ON a.codigoarticulo = df.codigoarticulo "+
			" LIMIT ? AND ? ";
	
}
