package co.edu.unal.pos;

import java.util.Arrays;

import org.kitesdk.data.Formats;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.dataset.DatasetDefinition;
import org.springframework.data.hadoop.store.dataset.DatasetOperations;
import org.springframework.data.hadoop.store.dataset.DatasetRepositoryFactory;
import org.springframework.data.hadoop.store.dataset.DatasetTemplate;
import org.springframework.data.hadoop.store.dataset.ParquetDatasetStoreWriter;

import co.edu.unal.pos.model.FactSales;

@Configuration
@ImportResource("hadoop-context.xml")
public class DatasetConfig {

	private @Autowired org.apache.hadoop.conf.Configuration hadoopConfiguration;

	@Bean
	public DatasetRepositoryFactory datasetRepositoryFactory() {
		DatasetRepositoryFactory datasetRepositoryFactory = new DatasetRepositoryFactory();
		datasetRepositoryFactory.setConf(hadoopConfiguration);
		datasetRepositoryFactory.setBasePath("/andres.rojas/spring");
		return datasetRepositoryFactory;
	}

    @Bean
    public DataStoreWriter<FactSales> dataStoreWriter() {
        return  new ParquetDatasetStoreWriter<FactSales>(FactSales.class, datasetRepositoryFactory(), fileInfoDatasetDefinition());
    }

	@Bean
	public DatasetOperations datasetOperations() {
		DatasetTemplate datasetOperations = new DatasetTemplate();
		datasetOperations.setDatasetDefinitions(Arrays.asList(fileInfoDatasetDefinition()));
		datasetOperations.setDatasetRepositoryFactory(datasetRepositoryFactory());
 		return datasetOperations;
	}

    @Bean
    public DatasetDefinition fileInfoDatasetDefinition() {
        DatasetDefinition definition = new DatasetDefinition();
        definition.setFormat(Formats.PARQUET.getName());
        definition.setTargetClass(FactSales.class);
        definition.setAllowNullValues(false);
        return definition;
    }
}