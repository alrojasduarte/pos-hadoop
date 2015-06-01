package co.edu.unal.pos.common.constants;

public interface HadoopPropertiesKeys {

	String NAME_SPACE="hadoop.";
	
	String NUBER_OF_WORKERS = NAME_SPACE+PropertiesKeys.NAME_SPACE+"numberOfWorkers";
	String BATCH_SIZE = NAME_SPACE+PropertiesKeys.NAME_SPACE+"batchSize";
	
	String DFS_URL = NAME_SPACE+PropertiesKeys.NAME_SPACE+"dfsUrl";
	String PATH_PATTERN = NAME_SPACE+PropertiesKeys.NAME_SPACE+"pathPattern";
	
	String FS_DEFAULT_FS = "fs.defaultFS";

	String POS_HADOOP_HOME = NAME_SPACE+PropertiesKeys.NAME_SPACE+"posHadoopHome";

	String FILE_FLUSH_SIZE = NAME_SPACE+PropertiesKeys.NAME_SPACE+"fileFlushSize";
	
}
