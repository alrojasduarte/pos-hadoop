package co.edu.unal.pos.common.constants;

public interface HadoopConstants {

	int NUMBER_OF_WORKERS = 5;
	int BATCH_SIZE = 5000;
	String DFS_URL = "webhdfs://localhost:50070";
	String PATH_PATTERN= "pos-hadoop-${uuid}.json";
	String PATH_UUID_WILDCARD = "\\$\\{uuid\\}";
	String POS_HADOOP_HOME = "/pos-hadoop/";
	int FILE_FLUSH_SIZE = 5000;
}
