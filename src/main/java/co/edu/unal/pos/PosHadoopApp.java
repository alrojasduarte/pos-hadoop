package co.edu.unal.pos;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.dataset.DatasetOperations;

import co.edu.unal.pos.model.FactSales;

/**
 * Hello world!
 *
 */
@ComponentScan
@EnableAutoConfiguration
public class PosHadoopApp implements CommandLineRunner {	
    
    public DatasetOperations datasetOperations;

	public DataStoreWriter<FactSales> writer;

	private long count;
	
    public static void main(String[] args) {
    	System.out.println( "Hello World!" );
    	SpringApplication.run(PosHadoopApp.class, args);
    	System.out.println( "Bye World!" );
    }

	@Override
	public void run(String... args) throws Exception {
		
			
	}

    @Autowired
    public void setDatasetOperations(DatasetOperations datasetOperations) {
        this.datasetOperations = datasetOperations;
    }

    @Autowired
    public void setDataStoreWriter(DataStoreWriter dataStoreWriter) {
        this.writer = dataStoreWriter;
    }

}
