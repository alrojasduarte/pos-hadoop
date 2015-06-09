package co.edu.unal.pos.common.constants;

public interface HadoopConstants {

	int NUMBER_OF_WORKERS = 5;
	int BATCH_SIZE = 5000;
	String DFS_URL = "hdfs://localhost:8020";
	String PATH_PATTERN= "pos-hadoop-${uuid}.json";
	String PATH_UUID_WILDCARD = "\\$\\{uuid\\}";
	String POS_HADOOP_HOME = "/pos-hadoop/";
	String POS_HADOOP_INPUT = POS_HADOOP_HOME+"input";
	String POS_HADOOP_OUTPUT = POS_HADOOP_HOME+"output";
	int FILE_FLUSH_SIZE = 5000;
	String POS_HADOOP_JOB_NAME="PosHadoopJob";
}
